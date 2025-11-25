from __future__ import annotations

from pathlib import Path
from typing import List

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from ..config import PulsarConfig
from ..models import SimpleForecaster
from ..store import TimeSeriesStore


def create_app(cfg: PulsarConfig) -> FastAPI:
    store = TimeSeriesStore(cfg.ensure_cache_dir() / "timeseries")
    forecaster = SimpleForecaster(store)
    template_root = Path(__file__).resolve().parents[3]
    template_path = template_root / "templates" / "forecast.html"
    if not template_path.exists():
        raise FileNotFoundError(f"template not found: {template_path}")
    template_html = template_path.read_text(encoding="utf-8")

    app = FastAPI(title="Pulsar Bridge API")

    def get_forecaster() -> SimpleForecaster:
        return forecaster

    @app.get("/", response_class=HTMLResponse)
    async def forecast_ui() -> str:
        return template_html

    @app.get("/healthz")
    async def health():
        return {"status": "ok"}

    @app.get("/forecast")
    async def forecast(
        hexagon: int = Query(..., description="H3 index as int"),
        service_type: int = Query(..., description="Snapp service type"),
        horizons: List[int] = Query([30, 60, 90]),
        fc: SimpleForecaster = Depends(get_forecaster),
    ):
        results = fc.forecast(hexagon, service_type, horizons)
        if not results:
            raise HTTPException(status_code=404, detail="not enough history for this hexagon")
        return [item.as_dict() for item in results]

    return app


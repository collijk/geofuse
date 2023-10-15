from pathlib import Path

from pydantic import BaseModel


class GeoFuseConfig(BaseModel):
    cache_path: str | Path = Path.home() / ".geofuse" / "cache"
    cache_verbose: int = 0
    google_api_key: str | None = None
    azure_api_key: str | None = None
    nominatim_user_agent: str | None = None

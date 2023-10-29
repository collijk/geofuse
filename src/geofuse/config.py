from pathlib import Path

from pydantic import BaseModel


class GeocodingCredentials(BaseModel):
    google_api_key: str | None = None
    azure_api_key: str | None = None
    nominatim_user_agent: str | None = None


class GeocodingCache(BaseModel):
    location: str | Path = Path.home() / ".geofuse" / "cache"
    verbose: int = 0


class Parameters(BaseModel):
    pass


class GeoFuseConfig(BaseModel):
    geocoding_credentials: GeocodingCredentials = GeocodingCredentials()
    geocoding_cache: GeocodingCache = GeocodingCache()
    parameters: Parameters = Parameters()

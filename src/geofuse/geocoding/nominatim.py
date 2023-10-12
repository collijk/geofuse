import time
from typing import Any

from geopy.geocoders import Nominatim
from joblib import Memory
from pydantic import BaseModel, ConfigDict, Field

from geofuse.config import GeoFuseConfig
from geofuse.geocoding.model import (
    BoundingBox,
    Geocoder,
    GeocodeRequest,
    GeocodeResponse,
    Point,
)


class NominatimGeocodeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str
    exactly_one: bool = False
    addressdetails: bool = False
    language: str = "en"
    geometry: str | None = None
    extratags: bool = False
    country_codes: str | list[str] | None = None
    viewbox: tuple | None = None
    bounded: bool = False
    featuretype: str | None = None
    namedetails: bool = False

    @classmethod
    def from_base_request(cls, request: GeocodeRequest) -> "NominatimGeocodeRequest":
        bbox = request.bounding_box
        if bbox:
            view_box = (
                (bbox.northwest.lat, bbox.northwest.lng),
                (bbox.southeast.lat, bbox.southeast.lng),
            )
        else:
            view_box = None

        country_codes = ["ke"] if request.country_iso3 else None

        return cls(
            query=request.query,
            country_codes=country_codes,
            viewbox=view_box,
        )


class NominatimGeocodeResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    place_id: int
    licence: str
    osm_type: str
    osm_id: int
    lat: str
    lon: str
    class_: str = Field(..., validation_alias="class")
    type: str
    place_rank: int
    importance: float
    addresstype: str
    name: str
    display_name: str
    boundingbox: list[str]

    def to_base_response(self, *args: Any, **kwargs: Any) -> GeocodeResponse:
        position = Point(
            lat=float(self.lat),
            lng=float(self.lon),
        )
        northwest = Point(
            lat=float(self.boundingbox[0]),
            lng=float(self.boundingbox[2]),
        )
        southeast = Point(
            lat=float(self.boundingbox[1]),
            lng=float(self.boundingbox[3]),
        )
        bbox = BoundingBox(northwest=northwest, southeast=southeast)  # type: ignore[call-arg]

        return GeocodeResponse(
            raw=self.model_dump(),
            name=self.name,
            position=position.to_shapely(),
            geometry=bbox.to_shapely(),
        )


def nominatim_geocode(client: Nominatim, *_: Any, **kwargs: Any) -> Any:
    start = time.time()
    results = client.geocode(**kwargs)
    request_frequency = 1  # per second
    request_period = (1 / request_frequency) * 1.1
    sleep_time = request_period - (time.time() - start)
    if sleep_time > 0:
        time.sleep(sleep_time)
    return results


class NominatimGeocoder(Geocoder):
    def __init__(self, config: GeoFuseConfig):
        memory = Memory(config.cache_path, verbose=config.cache_verbose)
        self._client = Nominatim(user_agent="kenya_census_geocode")
        self._geocode = memory.cache(ignore=["client"])(nominatim_geocode)

    @property
    def name(self) -> str:
        return "nominatim"

    def geocode(self, request: GeocodeRequest) -> list[GeocodeResponse]:
        nominatim_request = NominatimGeocodeRequest.from_base_request(request)
        response = self._geocode(self._client, **nominatim_request.model_dump())
        results = []
        for r in response:
            parsed = NominatimGeocodeResponse.model_validate(r.raw)
            results.append(parsed.to_base_response())

        return results

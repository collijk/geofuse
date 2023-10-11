import enum
from typing import Any, Literal

from azure.core.credentials import AzureKeyCredential
from azure.maps.search import MapsSearchClient
from azure.maps.search.models import BoundingBox as AzureBoundingBox
from joblib import Memory
from pydantic import AliasPath, BaseModel, ConfigDict, Field

from geofuse.config import GeoFuseConfig
from geofuse.geocoding.model import (
    BoundingBox,
    Geocoder,
    GeocodeRequest,
    GeocodeResponse,
    Point,
)


class AzureType(str, enum.Enum):
    geography = "Geography"
    cross_street = "Cross Street"
    street = "Street"
    point_address = "Point Address"
    address_range = "Address Range"


class AzureEntityType(str, enum.Enum):
    Country = "Country"
    CountrySecondarySubdivision = "CountrySecondarySubdivision"
    CountrySubdivision = "CountrySubdivision"
    CountryTertiarySubdivision = "CountryTertiarySubdivision"
    Municipality = "Municipality"
    MunicipalitySubdivision = "MunicipalitySubdivision"
    Neighbourhood = "Neighbourhood"
    PostalCodeArea = "PostalCodeArea"


class AzureAddress(BaseModel):
    model_config = ConfigDict(extra="forbid")

    freeform_address: str
    building_number: str | None = None
    country: str | None = None
    country_code: str | None = None
    country_code_iso3: str | None = None
    country_secondary_subdivision: str | None = None
    country_subdivision: str | None = None
    country_subdivision_name: str | None = None
    country_tertiary_subdivision: str | None = None
    cross_street: str | None = None
    extended_postal_code: str | None = None
    local_name: str | None = None
    municipality: str | None = None
    municipality_subdivision: str | None = None
    postal_code: str | None = None
    route_numbers: list[str] | None = None
    street: str | None = None
    street_name: str | None = None
    street_name_and_number: str | None = None
    street_number: str | None = None


class AzureGeocodeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str
    format: Literal["json", "xml"] = "json"
    is_type_ahead: bool | None = None
    top: int | None = None
    skip: int | None = None
    radius_in_meters: int | None = None
    coordinates: tuple[float, float] = (0.0, 0.0)
    country_filter: list[str] | None = None
    bounding_box: AzureBoundingBox | None = AzureBoundingBox()
    language: str | None = None
    extended_postal_codes_for: list[
        Literal["Addr", "Geo", "PAD", "POI", "Str", "XStr"]
    ] | None = None
    entity_type: str | None = None
    localized_map_view: str | None = None

    @classmethod
    def from_base_request(cls, request: GeocodeRequest) -> "AzureGeocodeRequest":
        country_filter = iso3_to_iso2(request.country_iso3)
        if request.bounding_box:
            bounding_box = AzureBoundingBox(
                west=request.bounding_box.northwest.lng,
                east=request.bounding_box.northeast.lng,
                north=request.bounding_box.northwest.lat,
                south=request.bounding_box.southwest.lat,
            )
        else:
            bounding_box = AzureBoundingBox()
        return cls(
            query=request.query,
            country_filter=country_filter,
            bounding_box=bounding_box,
        )


def iso3_to_iso2(iso3: str | None) -> list[str] | None:
    if iso3:
        return ["KE"]
    return None


class AzureGeocodeResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: AzureType
    id: str
    score: float
    distance_in_meters: float
    address: AzureAddress
    position: Point
    viewport: BoundingBox
    address_ranges: dict | None = None
    entry_points: list | None = None
    entity_type: AzureEntityType | None = None
    bounding_box: BoundingBox | None = Field(
        None, validation_alias=AliasPath("boundingBox")
    )
    match_score: float | None = Field(
        None, validation_alias=AliasPath("matchConfidence", "score")
    )
    geometry_id: str | None = Field(
        None, validation_alias=AliasPath("data_sources", "geometry", "id")
    )

    def to_base_response(self, *args: Any, **kwargs: Any) -> GeocodeResponse:
        if self.bounding_box is not None:
            geometry = self.bounding_box
        else:
            geometry = self.viewport
        return GeocodeResponse(
            raw=self.model_dump(),
            name=self.address.freeform_address,
            position=self.position.to_shapely(),
            geometry=geometry.to_shapely(),
        )


def azure_geocode(client: MapsSearchClient, *_: Any, **kwargs: Any) -> Any:
    return client.search_address(**kwargs)


class AzureGeocoder(Geocoder):
    def __init__(self, config: GeoFuseConfig):
        if config.azure_api_key is None:
            raise ValueError("Azure API key is not set.")
        memory = Memory(config.cache_path, verbose=config.cache_verbose)
        self._api_key = config.azure_api_key
        self._client = MapsSearchClient(credential=AzureKeyCredential(self._api_key))
        self._geocode = memory.cache(ignore=["client"])(azure_geocode)

    @property
    def name(self) -> str:
        return "azure"

    def geocode(self, request: GeocodeRequest) -> list[GeocodeResponse]:
        azure_request = AzureGeocodeRequest.from_base_request(request)
        response = self._geocode(self._client, **dict(azure_request))
        results = []
        for r in response.results:
            parsed = AzureGeocodeResponse.model_validate(r.as_dict())
            results.append(parsed.to_base_response())

        return results

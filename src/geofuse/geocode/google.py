import enum
from typing import Any

import googlemaps
from joblib import Memory
from pydantic import BaseModel, ConfigDict

from geofuse.config import GeoFuseConfig
from geofuse.geocode.model import (
    BoundingBox,
    Geocoder,
    GeocodeRequest,
    GeocodeResponse,
    Point,
)


class GoogleComponentType(str, enum.Enum):
    street_address = "street_address"
    route = "route"
    intersection = "intersection"
    political = "political"
    country = "country"
    administrative_area_level_1 = "administrative_area_level_1"
    administrative_area_level_2 = "administrative_area_level_2"
    administrative_area_level_3 = "administrative_area_level_3"
    administrative_area_level_4 = "administrative_area_level_4"
    administrative_area_level_5 = "administrative_area_level_5"
    administrative_area_level_6 = "administrative_area_level_6"
    administrative_area_level_7 = "administrative_area_level_7"
    colloquial_area = "colloquial_area"
    locality = "locality"
    sublocality = "sublocality"
    sublocality_level_1 = "sublocality_level_1"
    sublocality_level_2 = "sublocality_level_2"
    sublocality_level_3 = "sublocality_level_3"
    sublocality_level_4 = "sublocality_level_4"
    sublocality_level_5 = "sublocality_level_5"
    neighborhood = "neighborhood"
    premise = "premise"
    subpremise = "subpremise"
    plus_code = "plus_code"
    postal_code = "postal_code"
    natural_feature = "natural_feature"
    airport = "airport"
    park = "park"
    point_of_interest = "point_of_interest"
    floor = "floor"
    establishment = "establishment"
    landmark = "landmark"
    parking = "parking"
    post_box = "post_box"
    postal_town = "postal_town"
    room = "room"
    street_number = "street_number"
    bus_station = "bus_station"
    train_station = "train_station"
    transit_station = "transit_station"
    # Undocumented
    university = "university"
    lodging = "lodging"
    school = "school"
    primary_school = "primary_school"
    health = "health"
    hospital = "hospital"
    shopping_mall = "shopping_mall"
    clothing_store = "clothing_store"
    store = "store"
    cemetery = "cemetery"
    dentist = "dentist"
    food = "food"
    restaurant = "restaurant"
    pharmacy = "pharmacy"
    secondary_school = "secondary_school"
    postal_code_suffix = "postal_code_suffix"
    place_of_worship = "place_of_worship"
    church = "church"
    finance = "finance"
    tourist_attraction = "tourist_attraction"
    home_goods_store = "home_goods_store"
    grocery_or_supermarket = "grocery_or_supermarket"
    bakery = "bakery"
    travel_agency = "travel_agency"
    insurance_agency = "insurance_agency"
    museum = "museum"
    meal_takeaway = "meal_takeaway"
    hair_care = "hair_care"
    bar = "bar"
    doctor = "doctor"
    local_government_office = "local_government_office"
    gym = "gym"
    cafe = "cafe"
    hardware_store = "hardware_store"
    car_dealer = "car_dealer"
    police = "police"
    storage = "storage"
    hindu_temple = "hindu_temple"
    real_estate_agency = "real_estate_agency"
    campground = "campground"
    electronics_store = "electronics_store"
    general_contractor = "general_contractor"
    electrician = "electrician"
    plumber = "plumber"
    mosque = "mosque"
    town_square = "town_square"
    department_store = "department_store"
    convenience_store = "convenience_store"
    stadium = "stadium"
    car_wash = "car_wash"
    night_club = "night_club"
    rv_park = "rv_park"
    liquor_store = "liquor_store"
    zoo = "zoo"
    furniture_store = "furniture_store"
    supermarket = "supermarket"
    gas_station = "gas_station"
    postal_code_prefix = "postal_code_prefix"
    lawyer = "lawyer"
    jewelry_store = "jewelry_store"
    shoe_store = "shoe_store"
    art_gallery = "art_gallery"
    meal_delivery = "meal_delivery"
    drugstore = "drugstore"
    car_repair = "car_repair"
    amusement_park = "amusement_park"
    aquarium = "aquarium"
    movie_theater = "movie_theater"
    accounting = "accounting"
    beauty_salon = "beauty_salon"
    book_store = "book_store"
    veterinary_care = "veterinary_care"
    roofing_contractor = "roofing_contractor"


class GoogleLocationType(str, enum.Enum):
    ROOFTOP = "ROOFTOP"
    RANGE_INTERPOLATED = "RANGE_INTERPOLATED"
    GEOMETRIC_CENTER = "GEOMETRIC_CENTER"
    APPROXIMATE = "APPROXIMATE"


######################
# Google Base Models #
######################
# These models may be used in either requests or responses.


class GoogleAddressComponents(BaseModel):
    model_config = ConfigDict(extra="forbid")

    long_name: str
    short_name: str
    types: list[GoogleComponentType]


class GoogleGeometry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    location: Point
    location_type: GoogleLocationType
    viewport: BoundingBox
    bounds: BoundingBox | None = None


class GooglePlusCode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    global_code: str
    compound_code: str


class GoogleGeocodeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    address: str
    components: dict[GoogleComponentType, str] | None = None
    region: str | None = None
    bounds: BoundingBox | None = None

    @classmethod
    def from_base_request(cls, request: GeocodeRequest) -> "GoogleGeocodeRequest":
        return cls(
            address=request.query,
            region=request.country_iso3,
            bounds=request.bounding_box,
        )


class GoogleGeocodeResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    types: list[GoogleComponentType]
    formatted_address: str
    address_components: list[GoogleAddressComponents]
    geometry: GoogleGeometry
    place_id: str
    plus_code: GooglePlusCode | None = None
    partial_match: bool = False

    def to_base_response(self, *args: Any, **kwargs: Any) -> GeocodeResponse:
        if self.geometry.bounds is not None:
            geometry = self.geometry.bounds
        else:
            geometry = self.geometry.viewport
        return GeocodeResponse(
            raw=self.model_dump(),
            name=self.formatted_address,
            position=self.geometry.location.to_shapely(),
            geometry=geometry.to_shapely(),
        )


def google_geocode(client: googlemaps.Client, *_: Any, **kwargs: Any) -> Any:
    return client.geocode(**kwargs)


class GoogleGeocoder(Geocoder):
    def __init__(self, config: GeoFuseConfig):
        if config.geocoding_credentials.google_api_key is None:
            raise ValueError("Google API key is not set.")
        memory = Memory(**config.geocoding_cache.model_dump())
        self._api_key = config.geocoding_credentials.google_api_key
        self._client = googlemaps.Client(key=self._api_key)
        self._geocode = memory.cache(ignore=["client"])(google_geocode)

    @property
    def name(self) -> str:
        return "google"

    def geocode(self, request: GeocodeRequest) -> list[GeocodeResponse]:
        google_request = GoogleGeocodeRequest.from_base_request(request)
        response = self._geocode(self._client, **google_request.model_dump())
        results = []
        for r in response:
            parsed = GoogleGeocodeResponse.model_validate(r)
            results.append(parsed.to_base_response())

        return results

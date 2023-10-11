import abc

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator
from shapely import Point as Point_
from shapely import Polygon


class Point(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lat: float = Field(..., validation_alias=AliasChoices("lat", "latitude"))
    lng: float = Field(..., validation_alias=AliasChoices("lng", "lon", "longitude"))

    def to_shapely(self) -> Point_:
        return Point_(self.lng, self.lat)

    def to_qstring(self) -> str:
        return f"{self.lat}, {self.lng}"


class BoundingBox(BaseModel):
    model_config = ConfigDict(extra="forbid")

    northeast: Point = Field(
        None, validation_alias=AliasChoices("northeast", "top_right", "topRightPoint")
    )
    northwest: Point = Field(
        None, validation_alias=AliasChoices("northwest", "top_left", "topLeftPoint")
    )
    southeast: Point = Field(
        None,
        validation_alias=AliasChoices("southeast", "bottom_right", "btmRightPoint"),
    )
    southwest: Point = Field(
        None, validation_alias=AliasChoices("southwest", "bottom_left", "btmLeftPoint")
    )

    @model_validator(mode="after")
    def check_corners(self) -> "BoundingBox":
        if self.northeast and self.southwest:
            self.northwest = Point(lat=self.northeast.lat, lng=self.southwest.lng)
            self.southeast = Point(lat=self.southwest.lat, lng=self.northeast.lng)
        elif self.northwest and self.southeast:
            self.northeast = Point(lat=self.northwest.lat, lng=self.southeast.lng)
            self.southwest = Point(lat=self.southeast.lat, lng=self.northwest.lng)
        else:
            raise ValueError("BoundingBox must be initialized with opposite corners.")

        return self

    def to_shapely(self) -> Polygon:
        return Polygon(
            [
                p.to_shapely()
                for p in [
                    self.northeast,
                    self.southeast,
                    self.southwest,
                    self.northwest,
                ]
            ]
        )


class GeocodeRequest(BaseModel):
    """Common interface for geocoding requests."""

    model_config = ConfigDict(extra="forbid")

    query: str
    country_iso3: str | None = None
    bounding_box: BoundingBox | None = None


class GeocodeResponse(BaseModel):
    """Common interface for geocoding responses."""

    model_config = ConfigDict(
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    raw: dict
    name: str
    position: Point_
    geometry: Polygon = None


class Geocoder(abc.ABC):
    @abc.abstractmethod
    def geocode(self, request: GeocodeRequest) -> list[GeocodeResponse]:
        pass

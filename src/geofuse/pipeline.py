import unicodedata

import geopandas as gpd
import numpy as np
import pandas as pd

from geofuse import (
    AzureGeocoder,
    BoundingBox,
    GeocodeRequest,
    GeoFuseConfig,
    GoogleGeocoder,
    Point,
)


def make_path_to_top_parent(location_id: str, parent_map: pd.Series) -> str:
    path_to_top_parent = [location_id]
    while location_id != parent_map[location_id]:
        location_id = parent_map[location_id]
        path_to_top_parent.append(location_id)
    return ",".join([str(x) for x in reversed(path_to_top_parent)])


def normalize_string(s: str) -> str:
    lower_s = s.lower()
    ascii_s = unicodedata.normalize("NFKD", lower_s).encode("ascii", "ignore").decode()
    return ascii_s.replace("_", " ")


def check_if_parent(location: dict, parent: dict) -> None:
    if np.abs(parent["population_total"] - location["population_total"]) < 1e-6:
        raise NotImplementedError(
            f'{location["location_name_simple"]} likely same as parent {parent["location_name_simple"]}'
        )


def get_shapes_to_search(
    location: dict, current_shapes: pd.DataFrame, candidate_shapes: pd.DataFrame
) -> pd.DataFrame:
    bounding_geom = location["best_bounding_geometry"]
    mask = current_shapes.path_to_top_parent.str.contains(f"{bounding_geom},")
    shapes_to_search = current_shapes[mask]
    return shapes_to_search


def get_parent_bounds(
    location: dict, current_shapes: pd.DataFrame, candidate_shapes: pd.DataFrame
) -> BoundingBox:
    bounding_geom = location["best_bounding_geometry"]
    mask = current_shapes.path_to_top_parent == bounding_geom
    parent_bounds = current_shapes[mask].to_crs("EPSG:4326").total_bounds
    left, bottom, right, top = parent_bounds
    parent_bounds = BoundingBox(  # type: ignore[call-arg]
        northeast=Point(
            lat=top,
            lng=left,
        ),
        southwest=Point(
            lat=bottom,
            lng=right,
        ),
    )
    return parent_bounds


def geocode_location(
    config: GeoFuseConfig,
    location_name: str,
    country_code: str,
    parent_bounds: BoundingBox,
    parent_name: str | None = None,
) -> gpd.GeoDataFrame:
    geocoders = [
        ("google", GoogleGeocoder(config=config)),
        ("azure", AzureGeocoder(config=config)),
    ]
    address_formats = [
        ("loc", location_name.title()),
    ]
    if parent_name:
        address_formats.append(
            ("parent", f"{location_name.title()}, {parent_name.title()}")
        )

    boundary_formats = [
        ("F", None),
        ("T", parent_bounds),
    ]
    region_formats = [
        ("F", None),
        ("T", country_code),
    ]

    results = []
    for geocoder_label, geocoder in geocoders:
        for address_label, address in address_formats:
            for _bounds_label, bounds in boundary_formats:
                for _region_label, region in region_formats:
                    request = GeocodeRequest(
                        query=address,
                        country_iso3=region,
                        bounding_box=bounds,
                    )
                    response = geocoder.geocode(request)
                    inputs = [
                        geocoder_label,
                        address_label,
                        address,
                        region,
                        bounds.to_shapely() if bounds else None,
                    ]
                    if response:
                        for i, r in enumerate(response):
                            outputs = [i, r.name, r.position, r.geometry]
                            results.append(inputs + outputs)
                    else:
                        outputs = [0, None, None, None]
                        results.append(inputs + outputs)

    results_df = gpd.GeoDataFrame(
        results,
        columns=[
            "geocoder",
            "address_type",
            "address",
            "region",
            "bounds",
            "result_number",
            "location_name",
            "geometry",
            "result_bounds",
        ],
        geometry="geometry",
        crs="EPSG:4326",
    )
    results_df["parent_bounds"] = parent_bounds.to_shapely()
    results_df["inside_parent_bounds"] = results_df.within(parent_bounds.to_shapely())
    results_df["to_check"] = (
        results_df.geometry.notnull() & results_df.inside_parent_bounds
    )
    return results_df.to_crs("ESRI:54009")

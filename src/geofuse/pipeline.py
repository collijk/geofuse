import geopandas as gpd
import numpy as np
import pandas as pd
import pandera as pa

from geofuse import (
    AzureGeocoder,
    BoundingBox,
    Geocoder,
    GeocodeRequest,
    GeoFuseConfig,
    GoogleGeocoder,
    NominatimGeocoder,
    Point,
)


def prepare_input_data(
    input_df: pd.DataFrame,
    value_cols: list[str],
    best_bounding_geometry: tuple[str, str],
) -> pd.DataFrame:
    input_schema = pa.DataFrameSchema(
        columns={
            "location_id": pa.Column(str, unique=True, coerce=True),
            "parent_id": pa.Column(str, coerce=True),
            "location_name": pa.Column(str),
            "level": pa.Column(int, coerce=True),
            **{col: pa.Column(float, coerce=True) for col in value_cols},
        },
    )
    input_df = input_schema.validate(input_df)
    parent_map = input_df.set_index("location_id").parent_id
    bounding_loc_id, bounding_shape_id = best_bounding_geometry
    input_df["path_to_top_parent"] = input_df.location_id.apply(
        lambda x: make_path_to_top_parent(x, parent_map, bounding_loc_id),
    )
    input_df["best_bounding_geometry"] = bounding_shape_id
    input_df["geometry"] = None
    input_df = input_df.set_index("location_id")
    input_df.loc[bounding_loc_id, "geometry"] = bounding_shape_id
    return input_df.sort_values(["level", "path_to_top_parent"])


def make_path_to_top_parent(
    location_id: str, parent_map: pd.Series, top_location_id: str | None = None
) -> str:
    if top_location_id is None:
        top_mask = parent_map.index.values == parent_map.values
        assert top_mask.sum() == 1
        top_location_id = parent_map[top_mask].index[0]

    path_to_top_parent = [location_id]
    while location_id != top_location_id:
        location_id = parent_map[location_id]
        path_to_top_parent.append(location_id)
    return ",".join([str(x) for x in reversed(path_to_top_parent)])


def is_parent(location: dict, parent: dict, value_cols: list[str]) -> bool:
    tolerance = 1e-6
    return (
        all(np.abs(location[col] - parent[col]) < tolerance for col in value_cols)
        and location["level"] == parent["level"] + 1
    )


def get_shapes_to_search(
    location: dict,
    current_shapes: gpd.GeoDataFrame,
    candidate_shapes: gpd.GeoDataFrame,
    found_shapes: list[str],
) -> gpd.GeoDataFrame:
    bounding_geom = location["best_bounding_geometry"]
    mask = current_shapes.path_to_top_parent.str.contains(
        f"{bounding_geom},"
    ) & ~current_shapes.shape_id.isin(found_shapes)
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
    geocoders: list[Geocoder] = [
        GoogleGeocoder(config=config),
        AzureGeocoder(config=config),
        NominatimGeocoder(config=config),
    ]
    address_formats = [
        ("loc", location_name.title()),
    ]
    if parent_name:
        address_formats.append(
            ("parent", f"{location_name.title()}, {parent_name.title()}")
        )

    results = []
    for geocoder in geocoders:
        for address_label, address in address_formats:
            for bounds in [None, parent_bounds]:
                for region in [None, country_code]:
                    request = GeocodeRequest(
                        query=address,
                        country_iso3=region,
                        bounding_box=bounds,
                    )
                    try:
                        response = geocoder.geocode(request)
                    except Exception as e:
                        print(geocoder.name, " failure ", e)
                        response = None

                    inputs = [
                        geocoder.name,
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
    return results_df.to_crs("ESRI:54009")

import warnings

import geopandas as gpd
import numpy as np
import pandas as pd
import pandera as pa
from pandera.typing.geopandas import GeoSeries


class PartitionedSchema(pa.DataFrameModel):
    shape_id: str = pa.Field(nullable=True)
    parent_id: str
    path_to_top_parent: str
    level: int | float = pa.Field(nullable=True)
    geometry: GeoSeries


def determine_mergeability(
    gdf: gpd.GeoDataFrame,
    compactness_threshold: float = 0.05,
    detailed_area_threshold: float = 0.1,
    coarse_area_threshold: float = 0.1,
    keep_stats: bool = False,
) -> gpd.GeoDataFrame:
    """Determine which geometries are mergeable.

    This function determines which geometries in the provided GeoDataFrame are
    mergeable. A geometry is mergeable if it is missing from the admin
    geometries, if it is small relative to the parent geometry, or if it is
    small and not very compact.
    """
    keep_cols = [
        "parent_id",
        "path_to_top_parent",
        "shape_id",
        "level",
        "geometry",
    ]
    gdf = gdf.loc[:, keep_cols].copy()

    gdf["area"] = gdf.area
    gdf["bounding_area"] = gdf.minimum_bounding_circle().area
    gdf["compactness"] = gdf["area"] / gdf["bounding_area"]
    gdf["coarse_area"] = gdf.groupby("parent_id")["area"].transform("sum")
    gdf["detailed_area"] = gdf.groupby("shape_id")["area"].transform("sum")
    gdf["coarse_fraction"] = gdf["area"] / gdf["coarse_area"]
    gdf["detailed_fraction"] = gdf["area"] / gdf["detailed_area"]

    gdf["missing_from_admin"] = gdf["shape_id"].isnull()
    gdf["small_geometry"] = (gdf.detailed_fraction <= detailed_area_threshold) & (
        gdf.coarse_fraction <= coarse_area_threshold
    )
    gdf["sliver_geometry"] = (gdf.detailed_fraction <= 2 * detailed_area_threshold) & (
        gdf.compactness <= compactness_threshold
    )

    gdf["mergeable"] = (
        gdf["missing_from_admin"] | gdf["small_geometry"] | gdf["sliver_geometry"]
    )

    if not keep_stats:
        gdf = gdf.loc[:, keep_cols + ["mergeable"]].copy()
    return gdf


def collapse_mergeable_geoms(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    result = gdf.copy()
    result["merge_id"] = None
    result.loc[~result.mergeable, "merge_id"] = list(
        range(len(result.loc[~result.mergeable]))
    )

    # Split data into reference geoms (geoms we merge to)
    # and mergeable geoms (geoms we merge)
    reference = result.loc[~result.mergeable]
    mergeable = result.loc[result.mergeable]

    overlap = pd.DataFrame({"merge_id": None, "overlap": 0.0}, index=mergeable.index)
    for merge_id, g in reference.set_index("merge_id").geometry.items():
        # Our strategy is to buffer each reference geom, then assign
        # mergeable geoms to the reference geom with the biggest overlap.
        # Our buffer size is proportional to the "radius" of the reference
        # geom, which means we prefer to merge to a bigger geom in the
        # case of a tie.
        np.sqrt(g.area) / np.pi
        buffer_size = 10  # .0025 * radius
        buffered_geom = g.buffer(buffer_size)

        with warnings.catch_warnings():
            # Ignore shapely warnings where we compute an intersection
            # on geometries that don't intersect.
            warnings.simplefilter("ignore")
            shp_overlap = mergeable.intersection(buffered_geom)

        overlap_proportion = shp_overlap.area / mergeable.area
        new_match = overlap_proportion > overlap.overlap
        overlap.loc[new_match, "merge_id"] = merge_id
        overlap.loc[new_match, "overlap"] = overlap_proportion

    result.loc[overlap.index, "merge_id"] = overlap["merge_id"]
    result = result.dissolve(by="merge_id")["geometry"]

    reference = reference.set_index("merge_id")
    reference["geometry"] = result
    reference = gpd.GeoDataFrame(
        reference.reset_index().drop(columns=["mergeable", "merge_id"]),
        geometry="geometry",
        crs=gdf.crs,
    )

    return reference

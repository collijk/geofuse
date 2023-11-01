import warnings

import geopandas as gpd
import pandas as pd
import pandera as pa
from pandera.typing.geopandas import GeoSeries


class PartitionedSchema(pa.DataFrameModel):
    shape_id: str = pa.Field(nullable=True)
    shape_name: str = pa.Field(nullable=True)
    parent_id: str
    path_to_top_parent: str
    level: float = pa.Field(nullable=True)
    geometry: GeoSeries


class StatisticsSchema(PartitionedSchema):
    mergeable: bool


class FullStatisticsSchema(StatisticsSchema):
    area: float
    bounding_area: float
    compactness: float
    coarse_area: float
    coarse_fraction: float
    detailed_area: float
    detailed_fraction: float
    missing_from_admin: bool
    small_geometry: bool
    sliver_geometry: bool


def determine_mergeable_geometries(
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
    gdf: gpd.GeoDataFrame = PartitionedSchema.validate(gdf)  # type: ignore

    gdf["area"] = gdf.area

    gdf["bounding_area"] = gdf.minimum_bounding_circle().area
    gdf["compactness"] = gdf["area"] / gdf["bounding_area"]

    gdf["coarse_area"] = gdf.groupby("parent_id")["area"].transform("sum")
    gdf["coarse_fraction"] = gdf["area"] / gdf["coarse_area"]

    gdf["detailed_area"] = gdf.groupby("shape_id")["area"].transform("sum")
    gdf["detailed_fraction"] = gdf["area"] / gdf["detailed_area"]

    gdf["missing_from_admin"] = gdf["shape_id"].isnull()
    gdf["small_geometry"] = (gdf["detailed_fraction"] <= detailed_area_threshold) & (
        gdf["coarse_fraction"] <= coarse_area_threshold
    )
    gdf["sliver_geometry"] = (
        gdf["detailed_fraction"] <= 2 * detailed_area_threshold
    ) & (gdf["coarse_fraction"] <= compactness_threshold)

    gdf["mergeable"] = (
        gdf["missing_from_admin"] | gdf["small_geometry"] | gdf["sliver_geometry"]
    )

    out_schema = FullStatisticsSchema if keep_stats else StatisticsSchema
    gdf: gpd.GeoDataFrame = out_schema.validate(gdf)  # type: ignore

    return gdf


class CollapsableSchema(pa.DataFrameModel):
    shape_id: str = pa.Field(nullable=True)
    shape_name: str = pa.Field(nullable=True)
    parent_id: str
    path_to_top_parent: str
    level: float = pa.Field(nullable=True)
    geometry: GeoSeries
    mergeable: bool


class CollapsedSchema(pa.DataFrameModel):
    shape_id: str
    shape_name: str
    parent_id: str
    path_to_top_parent: str
    level: int = pa.Field(coerce=True)
    geometry: GeoSeries


def collapse_mergeable_geometries(
    gdf: gpd.GeoDataFrame,
    buffer_size: float = 10.0,
) -> gpd.GeoDataFrame:
    gdf: gpd.GeoDataFrame = CollapsableSchema.validate(gdf)  # type: ignore

    # Set merge_id to a unique ID for non-mergeable geoms and None for mergeable geoms
    gdf["merge_id"] = None
    gdf.loc[~gdf.mergeable, "merge_id"] = list(range(len(gdf.loc[~gdf.mergeable])))

    # Split data into reference geoms (geoms we merge to)
    # and mergeable geoms (geoms we merge)
    reference = gdf.loc[~gdf.mergeable]
    mergeable = gdf.loc[gdf.mergeable]

    overlap = pd.DataFrame({"merge_id": None, "overlap": 0.0}, index=mergeable.index)
    for merge_id, g in reference.set_index("merge_id").geometry.items():
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

    gdf.loc[overlap.index, "merge_id"] = overlap["merge_id"]
    result = gdf.dissolve(by="merge_id")["geometry"]

    reference = reference.set_index("merge_id")
    reference["geometry"] = result
    reference = reference.reset_index().drop(columns=["mergeable", "merge_id"])

    reference: gpd.GeoDataFrame = CollapsedSchema.validate(reference)  # type: ignore
    return reference

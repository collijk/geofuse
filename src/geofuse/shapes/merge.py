import warnings

import geopandas as gpd
import numpy as np
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

    class Config:
        strict = "filter"


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

    class Config:
        strict = "filter"


class CollapsedSchema(pa.DataFrameModel):
    shape_id: str
    shape_name: str
    parent_id: str
    path_to_top_parent: str
    level: int = pa.Field(coerce=True)
    geometry: GeoSeries

    class Config:
        strict = "filter"


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
        shp_overlap = qintersection(mergeable, buffered_geom)

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


def simple_merge(
    geometries: gpd.GeoDataFrame,
    threshold: float = 0.5,
    neighbor_count: int = 2,
) -> gpd.GeoDataFrame:
    if not geometries.mergeable.any():
        return geometries

    gdf = geometries.copy()
    mergeable = gdf.loc[gdf.mergeable].copy()

    neighbors_list = []
    for aid, row in mergeable.iterrows():
        g = row["geometry"]
        n = gdf.iloc[gdf.sindex.query(g)].drop(aid)
        n["nid"] = aid
        n["overlap"] = n.intersection(g.buffer(1)).area
        n["p_overlap"] = n["overlap"] / n["overlap"].sum()
        n["n_count"] = len(n[n["p_overlap"] > 0.01])
        if g.area <= 1e-3:
            n["p_overlap"] = 0.0
            n["very_small"] = [True] + [False] * (len(n) - 1)
        else:
            n["very_small"] = False
        neighbors_list.append(n)
    neighbors = pd.concat(neighbors_list)

    max_p = neighbors.groupby("nid")["p_overlap"].transform("max")
    threshold = np.maximum(max_p, threshold)

    meets_criteria = (neighbors["p_overlap"] >= threshold) & (
        neighbors["n_count"] <= neighbor_count
    )
    merge_map = (
        neighbors[neighbors["very_small"] | meets_criteria]
        .reset_index()
        .set_index("nid")
        .alg_id
    )
    drop = []
    areas = gdf.area.loc[merge_map.index]
    for idx, val in merge_map.items():
        if val in merge_map.index:
            if areas.loc[idx] > areas.loc[val]:
                drop.append(val)
            else:
                drop.append(idx)
    merge_map = merge_map.drop(drop)

    assert not merge_map.index.duplicated().any()
    gdf.loc[merge_map.index, "merge_id"] = merge_map

    new_gdf = (
        gdf.sort_values(["mergeable", "shape_name"])
        .dissolve(by="merge_id")
        .reset_index()
    )

    new_gdf["geometry"] = new_gdf["geometry"].buffer(1).buffer(-1)
    new_gdf["alg_id"] = new_gdf["merge_id"]
    new_gdf = new_gdf.set_index("alg_id")
    return new_gdf


def collapse_mergeable_geometries2(
    gdf: gpd.GeoDataFrame,
    buffer_size: float = 10.0,
) -> gpd.GeoDataFrame:
    gdf: gpd.GeoDataFrame = CollapsableSchema.validate(gdf)  # type: ignore

    gdf["alg_id"] = list(range(len(gdf)))
    gdf["merge_id"] = gdf["alg_id"]
    gdf = gdf.set_index("alg_id")

    threshold = 0.5
    neighbors = 2

    new_gdf = simple_merge(gdf)
    merged = len(new_gdf) - len(gdf)

    while gdf.mergeable.any():
        while merged:
            gdf, new_gdf = new_gdf, simple_merge(new_gdf, threshold, neighbors)
            merged = len(new_gdf) - len(gdf)

        if neighbors == 2:
            threshold = 0.7
            neighbors = 3
        elif neighbors == 3:
            if threshold > 0.4:
                threshold *= 0.9
            else:
                neighbors = 10000
                threshold = 0.7
        else:
            threshold *= 0.9

        gdf, new_gdf = new_gdf, simple_merge(new_gdf, threshold, neighbors)
        merged = len(new_gdf) - len(gdf)

    gdf = new_gdf.reset_index(drop=True)

    gdf: gpd.GeoDataFrame = CollapsedSchema.validate(gdf)  # type: ignore
    return gdf


def qintersection(left: gpd.GeoDataFrame, right: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    with warnings.catch_warnings():
        # Ignore shapely warnings where we compute an intersection
        # on geometries that don't intersect.
        warnings.simplefilter("ignore")
        intersection = left.intersection(right)
    return intersection

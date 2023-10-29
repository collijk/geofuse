import geopandas as gpd
import pandera as pa
from pandera.typing.geopandas import GeoSeries
from shapely.errors import GEOSException

from geofuse.model import DataFrameModel
from geofuse.shapes.retry import buffer_and_retry


class DetailedSchema(DataFrameModel):
    shape_id: str = pa.Field(unique=True)
    level: int
    geometry: GeoSeries


class CoarseSchema(DataFrameModel):
    shape_id: str = pa.Field(unique=True)
    path_to_top_parent: str = pa.Field(unique=True)
    level: int
    geometry: GeoSeries


class OutputSchema(DataFrameModel):
    shape_id: str = pa.Field(nullable=True)
    parent_id: str
    path_to_top_parent: str
    level: int | float = pa.Field(nullable=True)
    geometry: GeoSeries


def partition_geoms(
    coarse: gpd.GeoDataFrame,
    detailed: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Partition coarse geometries using detailed geometries as a guide.

    This function produces a GeoDataFrame that partitions (creates a
    collectively exhaustive and mutually representation) of each geometry in the
    provided coarse geometries. The partition is created by overlaying the
    detailed geometries on the coarse geometries. This operation can subdivide
    detailed geometries into multiple parts. It can also create new geometries
    that are not present in the detailed geometries but are present in the
    coarse geometries.

    Parameters
    ----------
    coarse
        The coarse geometries to partition.
    detailed
        The detailed geometries to use as a guide for partitioning.

    Returns
    -------
    gpd.GeoDataFrame
        A GeoDataFrame that partitions the coarse geometries.
    """
    coarse: gpd.GeoDataFrame = CoarseSchema.validate(coarse)  # type: ignore
    detailed: gpd.GeoDataFrame = DetailedSchema.validate(detailed)  # type: ignore

    @buffer_and_retry(GEOSException)
    def _overlay(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        return coarse.overlay(gdf, how="identity", keep_geom_type=True)

    union = _overlay(detailed)
    # Split up any multipolygons that were created by the overlay
    union = union.explode(index_parts=False).reset_index(drop=True)

    column_map = {
        "shape_id_1": "parent_id",
        "path_to_top_parent": "path_to_top_parent",
        "shape_id_2": "shape_id",
        "level_2": "level",
        "geometry": "geometry",
    }
    union = union.rename(columns=column_map)

    union: gpd.GeoDataFrame = OutputSchema.validate(union)  # type: ignore

    return union

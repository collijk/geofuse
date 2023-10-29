import geopandas as gpd
import pandera as pa
from loguru import logger
from pandera.typing.geopandas import GeoSeries
from shapely.geometry import MultiPolygon


class MultiPolygonInSchema(pa.DataFrameModel):
    geometry: GeoSeries


class MultiPolygonOutSchema(MultiPolygonInSchema):
    @pa.check("geometry")
    def _check_geometry(
        self,
        geometry: GeoSeries,
    ) -> GeoSeries:
        assert not geometry.apply(lambda g: isinstance(g, MultiPolygon)).any()
        return geometry


def fix_multipolygons(
    gdf: gpd.GeoDataFrame,
    start_buffer_size: float = 2**-16,
    max_buffer_size: float = 2**-8,
) -> gpd.GeoDataFrame:
    gdf: gpd.GeoDataFrame = MultiPolygonInSchema.validate(gdf)  # type: ignore

    trial_result = gdf["geometry"].copy()

    multipolygons = trial_result.apply(lambda g: isinstance(g, MultiPolygon))
    should_fail = False
    buffer_size = start_buffer_size
    while multipolygons.any():
        if buffer_size > max_buffer_size:
            if should_fail:
                raise RuntimeError(
                    "Multipolygon geometry still present after max buffer size reached"
                )
            else:
                # Try perturbing the buffer size to alter the resolution
                # of the geometry.
                buffer_size = start_buffer_size * 1.01
                should_fail = True

        logger.debug(f"Attempting to fix multipolygons with buffer size {buffer_size}.")

        test_result = trial_result.buffer(buffer_size).buffer(-buffer_size)
        collapsed = test_result.area == 0
        update = multipolygons & ~collapsed

        trial_result.loc[update] = test_result.loc[update]
        multipolygons = trial_result.apply(lambda g: isinstance(g, MultiPolygon))

        buffer_size *= 2

    gdf["geometry"] = trial_result

    gdf: gpd.GeoDataFrame = MultiPolygonOutSchema.validate(gdf)  # type: ignore
    return gdf

import geopandas as gpd
import numpy as np
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
    ) -> bool:
        return not geometry.apply(lambda g: isinstance(g, MultiPolygon)).any()
        


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


class OverlapInputSchema(pa.DataFrameModel):
    geometry: GeoSeries


class OverlapOutputSchema(OverlapInputSchema):
    @pa.check("geometry")
    def _check_geometry(
        self,
        geometry: GeoSeries,
    ) -> bool:
        total_area = geometry.area.sum()
        unary_union_area = geometry.unary_union.area
        err_tolerance = 1e-8
        return np.abs(total_area - unary_union_area) / unary_union_area < err_tolerance
        


def fix_overlapping_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf: gpd.GeoDataFrame = OverlapInputSchema.validate(gdf)  # type: ignore

    working_set = gdf["geometry"].tolist()
    ref_idx, other_idx = 0, 1
    broken = set()
    while ref_idx < len(working_set) - 1 and other_idx < len(working_set):
        ref = working_set[ref_idx]
        other = working_set[other_idx]

        # Remove overlaps with the reference geometry
        other = other.difference(ref)

        if isinstance(other, MultiPolygon):
            # If the other geometry is a multipolygon, the difference operation
            # did something we didn't want.  We'll try to fix it by reversing
            # the operation. This requires us to start over though as all other
            # geometry differences are now potentially invalid

            if (ref_idx, other_idx) in broken:
                # We've tried to fix before, don't try again
                logger.debug(
                    f"Unable to fix multipolygon difference "
                    f"between {ref_idx} and {other_idx}."
                )
                other_idx += 1
            else:
                logger.debug(
                    f"Attempting to fix multipolygon difference "
                    f"between {ref_idx} and {other_idx}."
                )
                working_set = gdf["geometry"].tolist()
                ref = working_set[ref_idx]
                other = working_set[other_idx]
                new_ref = ref.difference(other)

                working_set[ref_idx] = new_ref
                gdf.geometry.iloc[ref_idx] = new_ref

                broken.add((ref_idx, other_idx))

                ref_idx, other_idx = 0, 1
        else:
            # If the other geometry is not a multipolygon, we can continue
            # with the difference operation
            working_set[other_idx] = other
            other_idx += 1

        if other_idx == len(working_set):
            # We've reached the end of the working set, move on to the next
            # reference geometry
            ref_idx += 1
            other_idx = ref_idx + 1

    gdf["geometry"] = working_set

    gdf: gpd.GeoDataFrame = OverlapOutputSchema.validate(gdf)  # type: ignore

    return gdf

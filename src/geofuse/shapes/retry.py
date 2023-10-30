import functools
from typing import Callable, Type

import geopandas as gpd
from loguru import logger

ExceptionType = Type[Exception]
GDFTransformer = Callable[[gpd.GeoDataFrame], gpd.GeoDataFrame]


def buffer_on_exception(
    retry_on: ExceptionType | tuple[ExceptionType],
    buffer_start: float = 2**-16,
    max_buffer: float = 2**-8,
) -> Callable[[GDFTransformer], GDFTransformer]:
    def decorator(func: GDFTransformer) -> GDFTransformer:
        @functools.wraps(func)
        def wrapper(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
            gdf: gpd.GeoDataFrame = gdf.copy()  # type: ignore
            buffer = buffer_start
            should_fail = False
            while True:
                try:
                    return func(gdf)
                except retry_on as e:
                    if buffer > max_buffer:
                        if should_fail:
                            raise RuntimeError(
                                f"Caught exception {e} in {func.__name__}, "
                                f"max buffer size reached."
                            ) from e
                        else:
                            # Try perturbing the buffer size to alter the resolution
                            # of the geometry.
                            buffer = buffer_start * 1.01
                            should_fail = True
                    logger.debug(
                        f"Caught exception {e} in {func.__name__}, "
                        f"attempting to fix with buffer {buffer}"
                    )

                    gdf["geometry"] = gdf.buffer(buffer).buffer(-buffer)
                    buffer *= 2

        return wrapper

    return decorator

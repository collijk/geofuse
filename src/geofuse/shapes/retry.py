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


def buffer_on_condition(
    retry_on: Callable[[gpd.GeoDataFrame], bool],
    buffer_start: float = 2**-16,
    max_buffer: float = 2**-8,
) -> Callable[[GDFTransformer], GDFTransformer]:
    def decorator(func: GDFTransformer) -> GDFTransformer:
        @functools.wraps(func)
        def wrapper(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
            gdf: gpd.GeoDataFrame = gdf.copy()  # type: ignore
            buffer = buffer_start

            result = func(gdf)
            should_fail = False
            while True:
                if retry_on(result):
                    if buffer > max_buffer:
                        if should_fail:
                            raise RuntimeError(
                                f"Retry condition {retry_on.__name__} "
                                f"still met after max buffer size reached"
                            )
                        else:
                            # This multipolygon business is mainly
                            # about numerical precision, so we can try to get out
                            # of it by perturbing the buffer size.
                            buffer = buffer_start * 1.01
                            should_fail = True
                    logger.debug(
                        f"Retry condition {retry_on.__name__} met for {func.__name__}, "
                        f"attempting to fix with buffer {buffer}"
                    )

                    gdf["geometry"] = gdf.buffer(buffer).buffer(-buffer)
                    buffer *= 2
                    result = func(gdf)
                else:
                    return result

        return wrapper

    return decorator

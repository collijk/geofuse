import functools
from typing import Callable, Type

import geopandas as gpd
from loguru import logger

ExceptionType = Type[Exception]
GDFTransformer = Callable[[gpd.GeoDataFrame], gpd.GeoDataFrame]


def buffer_and_retry(
    retry_on: ExceptionType | tuple[ExceptionType],
    buffer_start: float = 2**-16,
    max_buffer: float = 2**-8,
) -> Callable[[GDFTransformer], GDFTransformer]:
    def decorator(func: GDFTransformer) -> GDFTransformer:
        @functools.wraps(func)
        def wrapper(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
            gdf: gpd.GeoDataFrame = gdf.copy()  # type: ignore
            buffer = buffer_start
            while True:
                try:
                    return func(gdf)
                except retry_on as e:
                    if buffer > max_buffer:
                        raise
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
            while True:
                if retry_on(result):
                    if buffer > max_buffer:
                        raise
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

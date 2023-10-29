import geopandas as gpd
from shapely.geometry import MultiPolygon


def clean_overlapping_geoms(data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = data.copy()

    i, j = 0, 1
    while i < len(gdf) - 1:
        shp = gdf.geometry.iloc[i]
        while j < len(gdf):
            o_shp = gdf.geometry.iloc[j]
            o_shp = o_shp.difference(shp)

            if isinstance(o_shp, MultiPolygon):
                gdf = data.copy()
                shp = gdf.geometry.iloc[i]
                o_shp = gdf.geometry.iloc[j]
                shp = shp.difference(o_shp)
                gdf.geometry.iloc[i] = shp
                i, j = 0, 1
                break

            gdf.geometry.iloc[j] = o_shp
            j += 1
        i += 1
        j = i + 1
    return gdf


def large_mergeable_area(
    gdf: gpd.GeoDataFrame,
    rel_threshold: float = 1e-3,
    abs_threshold: float = 1e-3,
    show_output: bool = False,
) -> bool:
    areas = gdf.dissolve(by="mergeable").area.to_frame()
    areas.columns = ["area"]
    areas["area"] /= 1000**2
    areas["proportion"] = areas["area"] / areas["area"].sum()
    areas = areas.T
    if True not in areas:
        areas[True] = 0.0

    af, at = areas.loc["area", False], areas.loc["area", True]
    pf, pt = areas.loc["proportion", False], areas.loc["proportion", True]

    def get_color(x: float, t: float) -> str:
        if x < t:
            return "green"
        elif x < 10 * t:
            return "yellow"
        else:
            return "red"

    rel_color = get_color(pt, rel_threshold)
    abs_color = get_color(at, abs_threshold)

    if show_output:
        print(
            f"Compact: {af:.3f} ({pf:.3f}), Mergeable: [{abs_color}]{at:.3f}[/{abs_color}] [{rel_color}]({pt:.3f})[/{rel_color}]"
        )

    return at >= abs_threshold

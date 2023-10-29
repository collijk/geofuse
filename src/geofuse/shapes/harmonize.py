import geopandas as gpd


def large_mergeable_area(
    gdf: gpd.GeoDataFrame,
    rel_threshold: float = 1e-3,
    abs_threshold: float = 1e-3,
    show_output: bool = False,
):
    areas = gdf.dissolve(by="mergeable").area.to_frame()
    areas.columns = ["area"]
    areas["area"] /= 1000**2
    areas["proportion"] = areas["area"] / areas["area"].sum()
    areas = areas.T
    if True not in areas:
        areas[True] = 0.0

    af, at = areas.loc["area", False], areas.loc["area", True]
    pf, pt = areas.loc["proportion", False], areas.loc["proportion", True]

    def get_color(x, t):
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


#
# parent_ids = a3.shape_id.unique().tolist()
#
# iterations_per_county = {}
# results = []
# start = time.time()
# for i, parent_id in enumerate(parent_ids):
#     elapsed = time.time() - start
#     itps = i / elapsed
#     remaining = (len(parent_ids) - (i+1))/itps if itps else np.inf
#     print(f'[green]STARTING ON {parent_id}[/green] | {i:>5}/{len(parent_ids):<5} {elapsed:.2f}s {i / elapsed:.2f}it/s {remaining/60:.2f}m remaining')
#     county = a3[a3.shape_id.str.contains(parent_id)]
#     a4s = a4_all[a4_all.parent_id.str.contains(parent_id)]
#     before = a4s.copy()
#
#     iterations = 0
#     while large_mergeable_area(a4s, show_output=True) and iterations < 10:
#         a4s = shapes.collapse_mergeable_geoms(a4s)
#         a4s = fix_multipolygons(a4s)f
#         a4s = do_overlay(county, a4s.drop(columns='path_to_top_parent'), verbose=True)
#         a4s = assign_mergeable(a4s)
#         iterations += 1
#
#     iterations_per_county[parent_id] = iterations
#     after = a4s.loc[~a4s.mergeable].drop(columns='mergeable')
#
#     carea = county.area.sum()
#     darea = a4s.area.sum()
#     start_area_err = 100 * (darea - carea) / carea
#     if area_err < 0.2:
#         after = clean_overlapping_geoms(after)
#         carea = county.area.sum()
#         darea = after.area.sum()
#         end_area_err = 100 * (darea - carea) / carea
#         color = 'red' if np.abs(end_area_err) > 0.0001 else 'green'
#         print(f"AREA ERROR start: [{color}]{start_area_err:.4f}[/{color}]% end:[{color}]{end_area_err:.4f}[/{color}]%")
#     else:
#         color = 'red'
#         print(f"AREA ERROR start: [{color}]{start_area_err:.4f}[/{color}]%")
#
#     results.append((county, before, after))

import geopandas as gpd
import pandas as pd
import tqdm

from geofuse import GeoFuseConfig, pipeline, scoring


def run_pipeline(
    input_df: pd.DataFrame,
    value_cols: list[str],
    best_bounding_geometry: tuple[str, str],
    current_shapes: gpd.GeoDataFrame,
    candidate_shapes: gpd.GeoDataFrame,
    config: GeoFuseConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pipeline.prepare_input_data(
        input_df,
        value_cols=value_cols,
        best_bounding_geometry=best_bounding_geometry,
    )

    geocoding_results = []
    for location_id in tqdm.tqdm(df.index[1:]):
        if df.loc[location_id, "geometry"] is not None:
            continue

        location = df.loc[location_id].to_dict()
        parent = df.loc[location["parent_id"]].to_dict()

        if pipeline.is_parent(location, parent, value_cols):
            continue

        # Subset search space to shapes in best bounding geometry
        shapes_to_search = pipeline.get_shapes_to_search(
            location=location,
            current_shapes=current_shapes,
            candidate_shapes=candidate_shapes,
        )
        if shapes_to_search.empty:
            continue

        parent_bounds = pipeline.get_parent_bounds(
            location=location,
            current_shapes=current_shapes,
            candidate_shapes=candidate_shapes,
        )

        # Build a scoring df with our candidate space.
        scores = shapes_to_search[["shape_name", "shape_id"]].copy()

        # Look for name matches in search space. Score the matches
        scores["name_score"] = scoring.score_names(
            location_name=location["location_name"],
            names_to_search=shapes_to_search.shape_name.tolist(),
        ).values

        # Look for area matches in the search space.  Score the matches
        # FIXME: Need to allow metrics on value cols.
        scores["area_score"] = scoring.score_area(
            location_area=location["land_area"],
            areas_to_search=shapes_to_search.area / 1000**2,
        )

        # Geocode the location within the search space.  Look for and score the matches.
        geocodes = pipeline.geocode_location(
            config=config,
            location_name=location["location_name"],
            country_code="KEN",
            parent_bounds=parent_bounds,
            parent_name=parent["location_name"],
        )
        shape_scores, geocode_scores = scoring.score_geocoding_results(
            geocodes,
            shapes_to_search,
            parent,
        )
        geocoding_results.append(geocode_scores)

        for c in shape_scores:
            scores[c] = shape_scores[c]
        weights = {
            "name_score": 1,
            "area_score": 2,
            "geocode_name_score": 0.5,
            "geocode_distance_score": 5,
        }
        scores["composite_score"] = scoring.compute_composite_score(scores, weights)

        cutoff = 0.1
        scores_below_cutoff = scores[scores.composite_score < cutoff]
        if scores_below_cutoff.empty:
            continue
        elif len(scores_below_cutoff) > 1:
            continue
        else:  # Single match
            # Update geometry
            shape_id = scores_below_cutoff["shape_id"].iloc[0]
            shape_pttp = shapes_to_search.set_index("shape_id").at[
                shape_id, "path_to_top_parent"
            ]
            df.at[location_id, "geometry"] = shape_pttp
            # Update children bounding geometry
            children = df.path_to_top_parent.str.contains(
                location["path_to_top_parent"]
            )
            df.loc[children, "best_bounding_geometry"] = shape_pttp

    return df, pd.concat(geocoding_results)

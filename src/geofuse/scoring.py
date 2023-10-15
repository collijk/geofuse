import itertools
import unicodedata

import numpy as np
import pandas as pd
from rapidfuzz import fuzz


def score_names(location_name: str, names_to_search: list[str]) -> pd.Series:
    results = []
    for name in names_to_search:
        results.append(
            fuzz.token_sort_ratio(location_name, name, processor=normalize_string)
        )
    return pd.Series(results, index=names_to_search)


def normalize_string(s: str) -> str:
    lower_s = s.lower()
    ascii_s = unicodedata.normalize("NFKD", lower_s).encode("ascii", "ignore").decode()
    return ascii_s.replace("_", " ")


def score_area(location_area: float, areas_to_search: pd.Series) -> np.ndarray:
    return np.maximum(
        1 - areas_to_search / location_area, 1 - location_area / areas_to_search
    )


def score_geocoding_results(
    geocodes: pd.DataFrame, shapes_to_search: pd.DataFrame, parent: dict
) -> tuple[pd.DataFrame, pd.DataFrame]:
    score_vars = ["name", "containment", "centroid", "boundary", "distance"]
    suffix_defaults = [("match", None), ("score", 1.0)]

    geocode_scores = pd.concat(
        [
            geocodes,
            pd.DataFrame(
                {
                    f"{var}_{suffix}": default
                    for var, (suffix, default) in itertools.product(
                        score_vars, suffix_defaults
                    )
                },
                index=geocodes.index,
            ),
        ],
        axis=1,
    )

    shape_scores = pd.DataFrame(
        {
            "geocode_name_score": 1.0,
            "geocode_distance_score": 1.0,
        },
        index=pd.Index(shapes_to_search.path_to_top_parent, name="path_to_top_parent"),
    )

    for idx, geocode in geocodes.iterrows():
        if not geocode["to_check"]:
            continue

        loc_name = geocode["location_name"]
        address = geocode["address"]
        loc = geocode["geometry"]

        address_score = score_names(
            location_name=address,
            names_to_search=shapes_to_search.shape_name.tolist(),
        )
        location_name_score = score_names(
            location_name=loc_name,
            names_to_search=shapes_to_search.shape_name.tolist(),
        )
        name_score = np.minimum(address_score, location_name_score)
        containment_score = (1 - shapes_to_search.contains(loc)).astype(float).values  # type: ignore[operator]
        centroid_score = np.minimum(
            shapes_to_search.centroid.distance(loc).values
            / 1000
            / np.sqrt(parent["land_area"]),
            1,
        )
        boundary_score = np.minimum(
            shapes_to_search.boundary.distance(loc).values
            / 1000
            / np.sqrt(parent["land_area"]),
            1,
        )
        distance_score = np.minimum(
            containment_score, np.minimum(centroid_score, boundary_score)
        )

        geocode_score = pd.DataFrame(
            {
                "name_score": name_score,
                "containment_score": containment_score,
                "centroid_score": centroid_score,
                "boundary_score": boundary_score,
                "distance_score": distance_score,
                "level": shapes_to_search.level.values,
            },
            index=pd.Index(
                shapes_to_search.path_to_top_parent, name="path_to_top_parent"
            ),
        )

        for score_var in score_vars:
            score_col, match_col = f"{score_var}_score", f"{score_var}_match"
            # Get the best shape id and score
            min_val = geocode_score[score_col].min()
            sorted_vals = geocode_score.loc[
                geocode_score[score_col] == min_val
            ].sort_values("level")
            geocode_scores.at[idx, score_col] = sorted_vals[score_col].iloc[-1]
            geocode_scores.at[idx, match_col] = sorted_vals.index[-1]

            # Accumulate the best location score.
            geocode_col = f"geocode_{score_col}"
            if geocode_col in shape_scores:
                shape_scores[geocode_col] = np.minimum(
                    shape_scores[geocode_col], geocode_score[score_col]
                )

    shape_scores.index = shapes_to_search.index

    return shape_scores, geocode_scores


def compute_composite_score(
    scores: pd.DataFrame, weights: dict[str, float]
) -> pd.Series:
    weights_df = pd.DataFrame(
        {name: weight for name, weight in weights.items() if name in scores},
        index=scores.index,
    )
    weights_df = weights_df.divide(weights_df.sum(axis=1), axis=0)
    return (weights_df * scores.loc[:, weights_df.columns]).sum(axis=1)  # type: ignore[index]

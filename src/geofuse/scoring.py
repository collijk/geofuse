import unicodedata

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from rapidfuzz import fuzz


def score_names(location_name: str, names_to_search: list[str]) -> pd.Series:
    results = []
    location_name = normalize_string(location_name)
    names_to_search = [normalize_string(n) for n in names_to_search]
    for name in names_to_search:
        name_results = [
            fuzz.token_sort_ratio(location_name, name),
            fuzz.token_sort_ratio(location_name.replace(' ', ''), name),
            fuzz.token_sort_ratio(location_name, name.replace(' ', '')),
            fuzz.token_sort_ratio(location_name.replace(' ', ''), name.replace(' ', ''))
        ]
        results.append(max(name_results))
    return 1 - pd.Series(results, index=names_to_search) / 100


def normalize_string(s: str) -> str:
    lower_s = s.lower()
    ascii_s = unicodedata.normalize("NFKD", lower_s).encode("ascii", "ignore").decode()
    strip_chars = ["_", "/", "-", "–"]
    for char in strip_chars:
        ascii_s = ascii_s.replace(char, " ")
    return ascii_s


def score_area(location_area: float, areas_to_search: pd.Series) -> np.ndarray:
    return np.maximum(
        1 - areas_to_search / location_area, 1 - location_area / areas_to_search
    )


def compute_composite_score(
    scores: pd.DataFrame, weights: dict[str, float]
) -> pd.Series:
    weights_df = pd.DataFrame(
        {name: weight for name, weight in weights.items() if name in scores},
        index=scores.index,
    )
    weights_df = weights_df.divide(weights_df.sum(axis=1), axis=0)
    return (weights_df * scores.loc[:, weights_df.columns]).sum(axis=1)  # type: ignore


def score_geocoding_results(
    geocodes: pd.DataFrame, shapes_to_search: gpd.GeoDataFrame, parent: dict
) -> pd.DataFrame:
    geocodes["confidence"] = _assess_geocode_confidence(geocodes)

    distance_scale = 1 / (1000 * np.sqrt(parent["land_area"]))
    centroids = shapes_to_search.centroid
    distance_score_cache = {}
    shape_scores = []
    for _, geocode in geocodes.iterrows():
        # Skip null geometries and locations outside the parent bounds.
        if pd.isnull(geocode["geometry"]):
            continue

        loc = geocode["geometry"]
        inside_parent_bounds = float(geocode['inside_parent_bounds'])

        if loc not in distance_score_cache:
            distance_score_cache[loc] = _score_geocoded_location(
                loc, inside_parent_bounds, shapes_to_search, centroids, distance_scale
            )
        geocode_score = distance_score_cache[loc].copy()
        geocode_score["confidence"] = geocode["confidence"]        
        shape_scores.append(geocode_score)

    out = pd.concat(shape_scores) if shape_scores else pd.DataFrame()

    return out


def _assess_geocode_confidence(geocodes: pd.DataFrame) -> np.ndarray:
    addresses = geocodes["address"].unique().tolist()
    name_scores = np.ones(len(geocodes), dtype=float)
    for address in addresses:
        address_score = score_names(address, geocodes["location_name"].fillna("").tolist())
        name_scores = np.minimum(name_scores, address_score.to_numpy())
    name_scores = np.exp(1 / (0.1 + name_scores))
    name_scores[geocodes['geometry'].isnull()] = 1.
    return np.maximum(name_scores, 1.)


def _score_geocoded_location(
    location: shapely.Point,
    inside_parent_bounds: float,
    shapes_to_search: gpd.GeoDataFrame,
    shape_centroids: gpd.GeoSeries,
    distance_scale: float,
) -> pd.DataFrame:
    scores = pd.DataFrame(
        {
            "containment": 1 - shapes_to_search.contains(location),
            "centroid": distance_scale * shape_centroids.distance(location),
            "boundary": distance_scale * shapes_to_search.boundary.distance(location),
        }
    )
    scores["distance"] = scores.min(axis=1) + 0.8 * (1 - inside_parent_bounds)
    scores["level"] = shapes_to_search["level"]
    scores["path_to_top_parent"] = shapes_to_search["path_to_top_parent"]
    return scores.set_index("path_to_top_parent")

import functools
import time

import geopandas as gpd
import pandas as pd

from geofuse.shapes.clean import fix_multipolygons, fix_overlapping_geometries
from geofuse.shapes.merge import (
    collapse_mergeable_geometries,
    determine_mergeable_geometries,
)
from geofuse.shapes.partition import partition_geometries
from geofuse.shapes.ui import HarmonizationUI


class Harmonizer:
    def __init__(
        self,
        location_name: str,
        coarse_admin_level: int,
        detailed_admin_level: int,
        coarse: gpd.GeoDataFrame,
        detailed: gpd.GeoDataFrame,
    ):
        self.coarse = coarse
        self.detailed = detailed
        self.parent_ids = self.coarse["shape_id"].unique().tolist()

        self.ui = HarmonizationUI(
            location_name=location_name,
            parent_ids=self.parent_ids,
            coarse_admin_level=coarse_admin_level,
            detailed_admin_level=detailed_admin_level,
        )

        self.metrics = {
            "partition_geometries": (0, 0.0),
            "determine_mergeable_geometries": (0, 0.0),
            "collapse_mergeable_geometries": (0, 0.0),
            "fix_multipolygons": (0, 0.0),
            "fix_overlapping_geometries": (0, 0.0),
        }
        self.partition_geometries = self.time_execution(partition_geometries)
        self.determine_mergeable_geometries = self.time_execution(
            determine_mergeable_geometries
        )
        self.collapse_mergeable_geometries = self.time_execution(
            collapse_mergeable_geometries
        )
        self.fix_multipolygons = self.time_execution(fix_multipolygons)
        self.fix_overlapping_geometries = self.time_execution(
            fix_overlapping_geometries
        )

    def time_execution(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()
            self.metrics[func.__name__][0] += 1
            self.metrics[func.__name__][1] += end - start
            return result

        return wrapper

    def step(self, coarse: gpd.GeoDataFrame, detailed: gpd.GeoDataFrame):
        detailed = self.collapse_mergeable_geometries(detailed)
        detailed = self.fix_multipolygons(detailed)
        detailed = self.partition_geometries(
            coarse, detailed.drop(columns="path_to_top_parent")
        )
        detailed = self.determine_mergeable_geometries(detailed)
        return detailed

    @staticmethod
    def compute_merge_statistics(gdf: gpd.GeoDataFrame):
        merge_area = gdf.dissolve(by="mergeable").area.to_frame()
        merge_area.columns = ["area"]
        merge_area["area"] /= 1000**2
        merge_area["percent"] = 100 * merge_area["area"] / merge_area["area"].sum()
        merge_area = merge_area.T
        if True not in merge_area:
            merge_area[True] = 0.0
        merge_area = merge_area.rename(columns={False: "reference", True: "mergeable"})
        return merge_area

    def run(self):
        max_iterations = 5
        partition = self.partition_geometries(self.coarse, self.detailed)
        partition = self.determine_mergeable_geometries(partition)

        parent_ids = partition["parent_id"].unique().tolist()
        results = []

        with self.ui:
            for i, parent_id in enumerate(parent_ids):
                start = time.time()
                coarse = self.coarse[self.coarse["shape_id"] == parent_id]
                detailed = partition[partition["parent_id"] == parent_id]

                merge_statistics = self.compute_merge_statistics(detailed)

                iterations = 0
                while (
                    iterations < max_iterations
                    and merge_statistics.at["area", "mergeable"] > 0.001
                ):
                    detailed = self.step(coarse, detailed)
                    merge_statistics = self.compute_merge_statistics(detailed)
                    iterations += 1

                detailed = detailed.loc[~detailed.mergeable].drop(columns="mergeable")

                coarse_area = coarse.area.sum()
                detailed_area = detailed.area.sum()

                start_err = 100 * (detailed_area - coarse_area) / coarse_area

                if start_err < 0.2:
                    detailed = self.fix_overlapping_geometries(detailed)
                    detailed_area = detailed.area.sum()
                    end_err = 100 * (detailed_area - coarse_area) / coarse_area
                else:
                    end_err = start_err

                results.append(detailed)

                end = time.time()

                metrics = (
                    i + 1,
                    parent_id,
                    merge_statistics.at["area", "reference"],
                    merge_statistics.at["percent", "reference"],
                    merge_statistics.at["area", "mergeable"],
                    merge_statistics.at["percent", "mergeable"],
                    iterations,
                    start_err,
                    end_err,
                    end - start,
                )
                self.ui.update(metrics)

        return gpd.GeoDataFrame(pd.concat(results, ignore_index=True))

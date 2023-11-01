import geopandas as gpd
import pandas as pd

from geofuse.shapes.clean import fix_multipolygons, fix_overlapping_geometries
from geofuse.shapes.merge import (
    collapse_mergeable_geometries,
    determine_mergeable_geometries,
)
from geofuse.shapes.model import AlgorithmMetrics, PerformanceMetrics
from geofuse.shapes.partition import partition_geometries
from geofuse.shapes.ui import HarmonizationUI
from loguru import logger


class Harmonizer:
    def __init__(
        self,
        location_name: str,
        coarse_admin_level: int,
        detailed_admin_level: int,
        coarse: gpd.GeoDataFrame,
        detailed: gpd.GeoDataFrame,
    ):
        coarse = coarse.explode(index_parts=True).reset_index(level=1)
        for col in ['shape_id', 'path_to_top_parent']:
            coarse[col] = coarse.apply(lambda row: f"{row[col]}_{row['level_1']}", axis=1)
        self.coarse = coarse.drop(columns=['level_1'])        
        
        self.detailed = detailed
        self.parent_ids = self.coarse["shape_id"].unique().tolist()
        self.max_step_iterations = 5

        self.a_metrics = AlgorithmMetrics()
        self.p_metrics = PerformanceMetrics()

        self.ui = HarmonizationUI(
            location_name=location_name,
            parent_ids=self.parent_ids,
            coarse_admin_level=coarse_admin_level,
            detailed_admin_level=detailed_admin_level,
            algorithm_metrics=self.a_metrics,
            performance_metrics=self.p_metrics,
        )

        self.partition_geometries = self.p_metrics.time_calls(partition_geometries)
        self.determine_mergeable_geometries = self.p_metrics.time_calls(
            determine_mergeable_geometries
        )
        self.collapse_mergeable_geometries = self.p_metrics.time_calls(
            collapse_mergeable_geometries
        )
        self.fix_multipolygons = self.p_metrics.time_calls(fix_multipolygons)
        self.fix_overlapping_geometries = self.p_metrics.time_calls(
            fix_overlapping_geometries
        )

        self.results = []

    def run(self) -> gpd.GeoDataFrame:
        if self.results:
            raise NotImplementedError

        self.ui.start()

        try:
            logger.info('Initializing...')
            partition = self.initialize()            

            for parent_id in self.parent_ids:
                logger.info(f'Starting {parent_id}')
                self.a_metrics.start_iteration(parent_id)

                coarse = self.coarse[self.coarse["shape_id"] == parent_id]
                detailed = partition[partition["parent_id"] == parent_id] 
                if detailed.mergeable.all():
                    detailed = detailed.dissolve(by=['parent_id', 'path_to_top_parent']).reset_index()
                    detailed['level'] = coarse['level'].iloc[0] + 1                    
                    detailed['mergeable'] = False                    

                detailed = self.collapse_geometries(coarse, detailed)
                detailed = detailed.loc[~detailed.mergeable].drop(columns="mergeable")

                detailed = self.correct_area(coarse, detailed)

                parent_id = parent_id.split('_')[0]
                detailed['parent_id'] = parent_id
                detailed['shape_id'] = [f"{parent_id}.{i+1}" for i in range(len(detailed))]
                detailed['path_to_top_parent'] = detailed.apply(lambda row: f"{row['path_to_top_parent'].split('_')[0]},{row['shape_id']}", axis=1)
                detailed['level'] = detailed['level'].astype(int)

                self.results.append(detailed)
                self.a_metrics.end_iteration()
                self.ui.update()
        except Exception as e:
            self.ui.stop()
            raise e

        self.ui.stop()

        return gpd.GeoDataFrame(pd.concat(self.results), crs=self.coarse.crs)

    def initialize(self) -> gpd.GeoDataFrame:
        partition = self.partition_geometries(self.coarse, self.detailed)
        partition = self.determine_mergeable_geometries(partition)
        return partition

    def collapse_geometries(
        self,
        coarse: gpd.GeoDataFrame,
        detailed: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        stats = self.a_metrics.start_collapse(detailed)

        while (
            stats["iterations"] < self.max_step_iterations
            and (stats["mergeable_area"] > 0.0001 or stats["mergeable_percent"] > 0.0001)
        ):
            detailed = self.collapse_mergeable_geometries(detailed)
            detailed = self.fix_multipolygons(detailed)
            detailed = self.partition_geometries(
                coarse, detailed.drop(columns="path_to_top_parent")
            )
            detailed = self.determine_mergeable_geometries(detailed)

            stats = self.a_metrics.end_collapse(detailed)

        stats = self.a_metrics.end_collapse(detailed)

        return detailed

    def correct_area(
        self, coarse: gpd.GeoDataFrame, detailed: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        area_error = self.a_metrics.start_area_correction(coarse, detailed)
        if area_error < 0.2:
            detailed = self.fix_overlapping_geometries(detailed)
        self.a_metrics.end_area_correction(coarse, detailed)
        return detailed

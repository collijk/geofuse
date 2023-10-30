from geofuse.shapes.clean import fix_multipolygons, fix_overlapping_geometries
from geofuse.shapes.merge import (
    collapse_mergeable_geometries,
    determine_mergeable_geometries,
)
from geofuse.shapes.partition import partition_geometries

from typing import Union

import pandera as pa

NullableID = Union[int, float]


class DataFrameModel(pa.DataFrameModel):
    """Base data frame model for setting global options."""

    class Config:
        strict = "filter"

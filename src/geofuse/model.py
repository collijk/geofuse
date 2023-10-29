import pandera as pa


class DataFrameModel(pa.DataFrameModel):
    """Base data frame model for setting global options."""

    class Config:
        strict = "filter"

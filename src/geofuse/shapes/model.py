import functools
import time
from collections import defaultdict
from typing import Any, Callable, ParamSpec, TypeVar

import geopandas as gpd
import numpy as np
from rich.panel import Panel
from rich.table import Table

T = TypeVar("T")
P = ParamSpec("P")


def _identity(s: Any) -> str:
    return str(s)


def _int(
    good_threshold: int | None = None, mediocre_threshold: int | None = None
) -> Callable[[int], str]:
    if mediocre_threshold is not None:
        assert good_threshold is not None
        assert good_threshold < mediocre_threshold

    if good_threshold is not None:
        default_color = "red"
    else:
        default_color = "white"

    def _format_int(i: int) -> str:
        int_str = f"{i}"
        if good_threshold is not None and i <= good_threshold:
            return f"[green]{int_str}[/green]"
        if mediocre_threshold is not None and i <= mediocre_threshold:
            return f"[yellow]{int_str}[/yellow]"
        return f"[bold {default_color}]{int_str}[/]"

    return _format_int


def _float(
    precision: int = 2,
    good_threshold: float | None = None,
    mediocre_threshold: float | None = None,
) -> Callable[[float], str]:
    if mediocre_threshold is not None:
        assert good_threshold is not None
        assert good_threshold < mediocre_threshold

    if good_threshold is not None:
        default_color = "red"
    else:
        default_color = "white"

    def _format_float(f: float) -> str:
        float_str = f"{f:.{precision}f}"
        if good_threshold is not None and np.abs(f) <= good_threshold:
            return f"[green]{float_str}[/]"
        if mediocre_threshold is not None and np.abs(f) <= mediocre_threshold:
            return f"[yellow]{float_str}[/]"
        return f"[bold {default_color}]{float_str}[/]"

    return _format_float


class AlgorithmMetrics:
    def __init__(self, display_rows: int = 10) -> None:
        self.metrics_properties = {
            "index": ({"header": "#"}, _identity),
            "parent_id": ({"header": "Parent ID"}, _identity),
            "reference_area_start": (None, None),
            "reference_percent_start": (None, None),
            "mergeable_area_start": (None, None),
            "mergeable_percent_start": (None, None),
            "reference_area_end": (
                {"header": "Reference", "justify": "right"},
                _float(1),
            ),
            "reference_percent_end": ({"header": "%"}, _float(1)),
            "mergeable_area_end": (
                {"header": "Mergeable", "justify": "right"},
                _float(3, 1e-3),
            ),
            "mergeable_percent_end": ({"header": "%"}, _float(3, 1e-3)),
            "iterations": ({"header": "Iterations", "justify": "right"}, _int(1, 4)),
            "area_error_start": (
                {"header": "AErr Pre (%)", "justify": "right"},
                _float(4, 1e-4, 1e-3),
            ),
            "area_error_end": (
                {"header": "AErr Post (%)", "justify": "right"},
                _float(4, 1e-4, 1e-3),
            ),
            "processing_time": (
                {"header": "Processing Time (s)", "justify": "right"},
                _float(2),
            ),
        }
        self.metrics: list[Any] = []
        self.current_row: dict[str, Any] = {}
        self.iteration_start_time = np.nan
        self.display_rows = display_rows

    def start_iteration(self, parent_id: str) -> None:
        self.current_row = {k: None for k in self.metrics_properties}
        self.current_row["index"] = len(self.metrics)
        self.current_row["parent_id"] = parent_id
        self.iteration_start_time = time.time()

    def end_iteration(self) -> None:
        self.current_row["processing_time"] = time.time() - self.iteration_start_time
        self.metrics.append(list(self.current_row.values()))
        self.current_row = {}
        self.iteration_start_time = np.nan

    def start_collapse(self, gdf: gpd.GeoDataFrame) -> dict[str, float]:
        stats = self.compute_merge_statistics(gdf)
        for k, v in stats.items():
            self.current_row[f"{k}_start"] = v
        self.current_row["iterations"] = 0
        stats["iterations"] = self.current_row["iterations"]
        return stats

    def end_collapse(self, gdf: gpd.GeoDataFrame) -> dict[str, float]:
        stats = self.compute_merge_statistics(gdf)
        for k, v in stats.items():
            self.current_row[f"{k}_end"] = v
        self.current_row["iterations"] += 1
        stats["iterations"] = self.current_row["iterations"]
        return stats

    def start_area_correction(
        self,
        coarse: gpd.GeoDataFrame,
        detailed: gpd.GeoDataFrame,
    ) -> float:
        area_error = self.compute_area_error(coarse, detailed)
        self.current_row["area_error_start"] = area_error
        return area_error

    def end_area_correction(
        self,
        coarse: gpd.GeoDataFrame,
        detailed: gpd.GeoDataFrame,
    ) -> float:
        area_error = self.compute_area_error(coarse, detailed)
        self.current_row["area_error_end"] = area_error
        return area_error

    @staticmethod
    def compute_merge_statistics(gdf: gpd.GeoDataFrame) -> dict[str, float]:
        merge_area = gdf.dissolve(by="mergeable").area.to_frame()
        merge_area.columns = ["area"]
        merge_area["area"] /= 1000**2
        merge_area["percent"] = 100 * merge_area["area"] / merge_area["area"].sum()
        merge_area = merge_area.T
        if True not in merge_area:
            merge_area[True] = 0.0
        merge_area = merge_area.rename(columns={False: "reference", True: "mergeable"})
        merge_area_dict = {
            "reference_area": merge_area.at["area", "reference"],
            "reference_percent": merge_area.at["percent", "reference"],
            "mergeable_area": merge_area.at["area", "mergeable"],
            "mergeable_percent": merge_area.at["percent", "mergeable"],
        }
        return merge_area_dict

    @staticmethod
    def compute_area_error(
        coarse: gpd.GeoDataFrame, detailed: gpd.GeoDataFrame
    ) -> float:
        coarse_area = coarse.area.sum()
        detailed_area = detailed.area.sum()
        return 100 * (detailed_area - coarse_area) / coarse_area

    def __rich__(self) -> Panel:
        table = Table.grid(padding=1, expand=True)
        for column_args, _ in self.metrics_properties.values():
            if column_args is not None:
                table.add_column(**column_args)  # type: ignore[arg-type]
        header = [
            f"[b]{c[0]['header']}[/b]"
            for c in self.metrics_properties.values()
            if c[0] is not None
        ]
        table.add_row(*header)

        for row in self.metrics[-self.display_rows :]:
            out_row = []
            for element, (_, formatter) in zip(row, self.metrics_properties.values()):
                if formatter is not None:
                    out_row.append(formatter(element))
            table.add_row(*out_row)

        return Panel(
            table, title="Harmonization Metrics", border_style="cyan", padding=(1, 2)
        )


class PerformanceMetrics:
    def __init__(self) -> None:
        self.metrics: dict[str, list] = defaultdict(lambda: [0, 0.0])

    def time_calls(self, func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()
            self.metrics[func.__name__][0] += 1
            self.metrics[func.__name__][1] += end - start
            return result

        return wrapper

    def __rich__(self) -> Panel:
        table = Table.grid(padding=1, expand=True)
        table.add_column("Function", justify="left")
        table.add_column("Calls", justify="right")
        table.add_column("Time (s)", justify="right")
        table.add_column("Time per Call (s)", justify="right")
        table.add_column("%", justify="right")

        total_time = sum(t for _, (_, t) in self.metrics.items())
        if total_time > 0:
            max_percent_t = max([t / total_time for _, (_, t) in self.metrics.items()])
        else:
            max_percent_t = np.nan

        for func_name, (calls, t) in self.metrics.items():
            t_per_call = f"{t / calls:.3f}" if calls > 0 else "N/A"
            percent_t = f"{100*t / total_time:.1f}" if total_time > 0 else "N/A"

            if percent_t == max_percent_t:
                func_name = f"[bold]{func_name}[/]"
                percent_t = f"[bold yellow]{percent_t}[/]"

            table.add_row(
                func_name,
                f"{calls:,}",
                f"{t:.2f}",
                t_per_call,
                percent_t,
            )

        return Panel(
            table,
            title="Harmonization Performance",
            border_style="cyan",
            padding=(1, 2),
        )

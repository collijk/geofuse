from datetime import datetime

from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from geofuse.shapes.model import AlgorithmMetrics, PerformanceMetrics


class HarmonizationHeader:
    def __init__(
        self,
        location_name: str,
        coarse_admin_level: int,
        detailed_admin_level: int,
    ) -> None:
        self.location_name = location_name
        self.coarse_admin_level = coarse_admin_level
        self.detailed_admin_level = detailed_admin_level

    def __rich__(self) -> Panel:
        grid = Table.grid(expand=True)
        grid.add_column(justify="left", ratio=1)
        grid.add_column(justify="right", ratio=1)
        title = (
            f"[b]Geofuse Shape Harmonization[/b] "
            f"- [i]{self.location_name}[/i] "
            f"- Admin {self.detailed_admin_level} to Admin {self.coarse_admin_level}"
        )
        clock = datetime.now().ctime().replace(":", "[blink]:[/]")
        grid.add_row(title, clock)
        return Panel(grid, style="cyan")


class HarmonizationProgress:
    def __init__(self, parent_ids: list[str]) -> None:
        self.progress = Progress(
            TextColumn("{task.fields[shape_id]}"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("[progress.remaining]{task.completed}/{task.total}"),
            SpinnerColumn(),
            BarColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            expand=True,
        )

        self.parent_ids = parent_ids
        self.task_index = 0

        self.task = self.progress.add_task(
            "Harmonizing",
            total=len(parent_ids),
            shape_id=self.parent_ids[self.task_index],
        )

    def advance(self) -> None:
        self.task_index += 1
        self.progress.update(
            self.task, advance=1, shape_id=self.parent_ids[self.task_index]
        )

    def __rich__(self) -> Panel:
        return Panel(
            self.progress,
            title="Harmonization Progress",
            border_style="cyan",
            padding=(1, 2),
        )


class HarmonizationUI:
    def __init__(
        self,
        location_name: str,
        parent_ids: list[str],
        coarse_admin_level: int,
        detailed_admin_level: int,
        algorithm_metrics: AlgorithmMetrics,
        performance_metrics: PerformanceMetrics,
    ) -> None:
        self.header = HarmonizationHeader(
            location_name=location_name,
            coarse_admin_level=coarse_admin_level,
            detailed_admin_level=detailed_admin_level,
        )
        self.progress = HarmonizationProgress(parent_ids)
        self.algorithm_metrics = algorithm_metrics
        self.performance_metrics = performance_metrics

        self.layout = Layout(visible=False)

        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="progress", size=5),
            Layout(name="algorithm_metrics", size=25),
            Layout(name="performance_metrics", size=16),
        )
        self.layout["header"].update(self.header)
        self.layout["progress"].update(self.progress)
        self.layout["algorithm_metrics"].update(self.algorithm_metrics)
        self.layout["performance_metrics"].update(self.performance_metrics)
        self.instance = Live(self.layout, refresh_per_second=10)

    def update(self) -> None:
        self.progress.advance()

    def start(self) -> None:
        self.instance.start()

    def stop(self) -> None:
        self.instance.stop()
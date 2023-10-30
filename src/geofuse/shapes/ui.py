import datetime

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


class HarmonizationHeader:
    def __init__(
        self,
        location_name: str,
        coarse_admin_level: int,
        detailed_admin_level: int,
    ):
        self.location_name = location_name
        self.coarse_admin_level = coarse_admin_level
        self.detailed_admin_level = detailed_admin_level

    def __rich__(self):
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
        return Panel(grid, style="cyan on black")


class HarmonizationProgress:
    def __init__(self, parent_ids: list[str]):
        self.progress = Progress(
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("[progress.remaining]{task.completed}/{task.total}"),
            SpinnerColumn(),
            BarColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            expand=True,
        )
        self.task = self.progress.add_task("Harmonizing", total=len(parent_ids))

        self.parent_ids = parent_ids
        self.task_index = 0
        self.progress.start()
        self.progress.console.print(f"Working on {self.parent_ids[self.task_index]}")

    def advance(self):
        self.task_index += 1
        self.progress.advance(self.task)
        self.progress.console.print(f"Working on {self.parent_ids[self.task_index]}")

    def __rich__(self):
        return Panel(
            self.progress,
            title="Harmonization Progress",
            border_style="cyan",
            padding=(1, 2),
        )


class HarmonizationMetrics:
    def __init__(self, rows: int = 10):
        self.rows = rows
        self.columns = [
            {"header": "#"},
            {"header": "Parent ID"},
            {"header": "Reference"},
            {"header": "%"},
            {"header": "Mergeable"},
            {"header": "%"},
            {"header": "Iterations"},
            {"header": "Error"},
            {"header": "Post-fix Error"},
            {"header": "Processing Time (s)"},
        ]
        self.metrics = []

    def update(self, metrics):
        self.metrics.append(metrics)
        if len(self.metrics) > self.rows:
            self.metrics.pop(0)

    def __rich__(self):
        table = Table.grid(padding=1)
        for column in self.columns:
            table.add_column(**column)

        for row in self.metrics:
            table.add_row(*row)

        return Panel(
            table, title="Harmonization Metrics", border_style="cyan", padding=(1, 2)
        )


class HamonizationUI:
    def __int__(
        self,
        location_name: str,
        parent_ids: list[str],
        coarse_admin_level: int,
        detailed_admin_level: int,
    ):
        self.running = False
        self.header = HarmonizationHeader(
            location_name=location_name,
            coarse_admin_level=coarse_admin_level,
            detailed_admin_level=detailed_admin_level,
        )
        self.progress = HarmonizationProgress(parent_ids)
        self.metrics = HarmonizationMetrics()

        self.layout = Layout()
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="progress", size=5),
            Layout(name="metrics", size=15),
        )
        self.layout["header"].update(self.header)
        self.layout["progress"].update(self.progress)
        self.layout["metrics"].update(self.metrics)

    def update(self, metrics):
        self.progress.advance()
        self.metrics.update(metrics)

    def __enter__(self):
        self.running = True
        with Live(self.layout, refresh_per_second=4) as live:
            return live

    def __exit__(self, exc_type, exc_value, traceback):
        self.running = False

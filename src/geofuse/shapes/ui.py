from datetime import datetime

import numpy as np
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
        return Panel(grid, style="cyan")


class HarmonizationProgress:
    def __init__(self, parent_ids: list[str]):
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

        

    def advance(self):
        self.task_index += 1
        self.progress.update(self.task, advance=1, shape_id=self.parent_ids[self.task_index])

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
            {"header": "#", "style": "green"},
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
        table = Table.grid(padding=1, expand=True)
        for column in self.columns:
            table.add_column(**column)
        header = [f"[b]{c['header']}[/b]" for c in self.columns]
        table.add_row(*header)

        for row in self.metrics:
            out_row = []
            for v in row:
                if isinstance(v, float):
                    out_row.append(np.format_float_positional(
                        v, 
                        precision=3, 
                        unique=False,
                        fractional=False,
                    ))
                else:
                    out_row.append(str(v))
            
            row = [str(v) for v in row]
            table.add_row(*row)

        return Panel(
            table, title="Harmonization Metrics", border_style="cyan", padding=(1, 2)
        )


class HarmonizationUI:
    def __init__(
        self,
        location_name: str,
        parent_ids: list[str],
        coarse_admin_level: int,
        detailed_admin_level: int,
    ):
        self.instance = None
        self.header = HarmonizationHeader(
            location_name=location_name,
            coarse_admin_level=coarse_admin_level,
            detailed_admin_level=detailed_admin_level,
        )
        self.progress = HarmonizationProgress(parent_ids)
        self.metrics = HarmonizationMetrics()

        self.layout = Layout(visible=False)
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="progress", size=5),
            Layout(name="metrics", size=25),
        )
        self.layout["header"].update(self.header)
        self.layout["progress"].update(self.progress)
        self.layout["metrics"].update(self.metrics)

    def update(self, metrics):
        self.progress.advance()
        self.metrics.update(metrics)

    def __enter__(self):        
        self.instance = Live(self.layout, refresh_per_second=10)
        self.instance.start()        
        return self.instance

    def __exit__(self, exc_type, exc_value, traceback):
        self.instance.stop()
        self.instance = None

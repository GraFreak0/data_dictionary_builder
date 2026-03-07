"""
ExecutionTimer
==============
Lightweight task-level timing utility for data_dictionary_builder.

Usage
-----
::

    from data_dictionary_builder import ExecutionTimer

    timer = ExecutionTimer()

    with timer.task("Connect & extract"):
        with MetadataExtractor(**config) as ext:
            db_meta = ext.extract_all_schemas(parallel_workers=8)

    with timer.task("Generate YAML"):
        gen.generate_yaml_files(db_meta)

    with timer.task("Compare schemas"):
        report = comparator.compare_and_generate_report("public")

    timer.summary()
    # ────────────────────────────────────
    #   Execution Summary
    # ────────────────────────────────────
    #   Connect & extract          2.341s
    #   Generate YAML              0.087s
    #   Compare schemas            1.203s
    # ────────────────────────────────────
    #   TOTAL                      3.631s
    # ────────────────────────────────────
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Generator, List, Tuple


class ExecutionTimer:
    """
    Tracks elapsed time for individually named tasks and the overall run.

    The timer starts as soon as the instance is created.  Wrap each logical
    step with the :meth:`task` context manager to capture its duration.
    Call :meth:`summary` at the end to print a formatted report to stdout.

    Parameters
    ----------
    (none — the clock starts on construction)

    Examples
    --------
    ::

        timer = ExecutionTimer()

        with timer.task("Extract metadata"):
            db_meta = extractor.extract_all_schemas()

        with timer.task("Generate YAML"):
            gen.generate_yaml_files(db_meta)

        timer.summary()

        # Access raw timings programmatically:
        tasks, total = timer.totals()
        for name, secs in tasks:
            print(f"{name}: {secs:.3f}s")
    """

    def __init__(self) -> None:
        self._start: float = time.perf_counter()
        self._tasks: List[Tuple[str, float]] = []

    # ------------------------------------------------------------------ #
    # Context manager                                                       #
    # ------------------------------------------------------------------ #

    @contextmanager
    def task(self, name: str) -> Generator[None, None, None]:
        """
        Context manager that measures wall-clock time for a named block.

        The recorded duration is appended to the internal task list
        regardless of whether the block raises an exception.

        Parameters
        ----------
        name : str
            Human-readable label displayed in :meth:`summary`.

        Examples
        --------
        ::

            with timer.task("Database connection"):
                ext.connect()
        """
        t0 = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - t0
            self._tasks.append((name, elapsed))

    # ------------------------------------------------------------------ #
    # Results                                                              #
    # ------------------------------------------------------------------ #

    @property
    def elapsed(self) -> float:
        """Seconds elapsed since this :class:`ExecutionTimer` was created."""
        return time.perf_counter() - self._start

    def totals(self) -> Tuple[List[Tuple[str, float]], float]:
        """
        Return ``(task_timings, overall_elapsed_seconds)``.

        ``task_timings`` is a list of ``(name, seconds)`` tuples in
        chronological order (one entry per :meth:`task` block that has
        completed).

        Returns
        -------
        tuple[list[tuple[str, float]], float]
        """
        return list(self._tasks), self.elapsed

    def summary(self, title: str = "Execution Summary") -> None:
        """
        Print a formatted timing table to stdout.

        Each completed :meth:`task` appears as one row.  The last row shows
        the overall wall-clock time since the timer was created (which may
        be slightly longer than the sum of individual tasks if work was done
        outside a named block).

        Parameters
        ----------
        title : str
            Header line of the summary table.  Defaults to
            ``"Execution Summary"``.
        """
        overall = self.elapsed
        col_width = max((len(n) for n, _ in self._tasks), default=20) + 4
        total_width = col_width + 12
        bar = "─" * total_width

        print(f"\n{bar}")
        print(f"  {title}")
        print(bar)
        for name, secs in self._tasks:
            label = f"  {name}".ljust(col_width)
            print(f"{label}{_fmt(secs):>10}")
        print(bar)
        total_label = "  TOTAL".ljust(col_width)
        print(f"{total_label}{_fmt(overall):>10}")
        print(f"{bar}\n")


# ------------------------------------------------------------------ #
# Internal helpers                                                     #
# ------------------------------------------------------------------ #

def _fmt(seconds: float) -> str:
    """Return *seconds* as a compact human-readable string."""
    if seconds < 60:
        return f"{seconds:.3f}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {secs:.1f}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {secs:.0f}s"

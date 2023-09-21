# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""CLI entry point to test individual parts of the code."""
import concurrent.futures
import os
import pathlib
import random
from typing import Callable

import click

from py_spanner_mutex import _main_import
from py_spanner_mutex.common import logger

_LOGGER = logger.get(__name__)
_RAND: random.Random = random.Random(123)


@click.group(help="CLI entry point -- Test the critical section/mutex.")
def cli() -> None:
    """Click entry-point."""


@cli.command(help="Single client trying to execute the critical section")
@click.option(
    "--config-file",
    required=True,
    help="JSON file with Spanner mutex configuration",
    type=click.Path(file_okay=True, dir_okay=False, resolve_path=True, path_type=pathlib.Path),
)
def single_client(config_file: pathlib.Path) -> None:
    _LOGGER.info("Starting single client critical section using: '%s'", locals())
    mutex_obj = _main_import.simple_mutex(config_file)
    mutex_obj.start()
    _LOGGER.info("Ended critical section for '%s' and result in '%s'", mutex_obj, mutex_obj.target_filename)


@cli.command(help="Multiple clients trying to execute the critical section")
@click.option(
    "--config-file",
    required=True,
    help="JSON file with Spanner mutex configuration",
    type=click.Path(file_okay=True, dir_okay=False, resolve_path=True, path_type=pathlib.Path),
)
@click.option(
    "--proc",
    help="Amount of processes. Default is the amount of cores.",
    required=False,
    default=os.cpu_count(),
    type=int,
)
@click.option(
    "--client-proc",
    help="Amount of clients/threads per process",
    required=False,
    default=10,
    type=int,
)
def multi_client(config_file: pathlib.Path, proc: int, client_proc: int) -> None:
    _LOGGER.info("Starting threaded multiple clients critical section using: '%s'", locals())
    config = _main_import.config_obj(config_file)
    target_filename = _main_import.temp_filename()
    proc = max(1, proc)
    client_proc = max(1, client_proc)
    amount_threads = proc * client_proc
    _proc_multi_client(
        proc,
        client_proc,
        _main_import.create_and_start_simple_mutex_client,
        config=config,
        target_filename=target_filename,
    )
    _LOGGER.info("Ended critical section for %d clients and result in '%s'", amount_threads, target_filename)


def _proc_multi_client(max_workers_proc: int, max_workers_thr: int, func: Callable, *args, **kwargs) -> None:
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers_proc) as executor:
        future_clients = [
            executor.submit(_main_import.threaded_multi_client, max_workers_thr, func, *args, **kwargs)
            for _ in range(max_workers_proc)
        ]
        _ = concurrent.futures.wait(future_clients)


def main():
    cli()


if __name__ == "__main__":
    main()

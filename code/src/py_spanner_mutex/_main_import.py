# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""Because multiprocessing has a hard time with non-imported targets, using this as a workaround."""
import concurrent.futures
import os
import pathlib
import random
import tempfile
import threading
import time
import uuid
from datetime import datetime
from typing import Callable, Optional

from py_spanner_mutex import spanner_mutex
from py_spanner_mutex.common import logger, preprocess
from py_spanner_mutex.dto import mutex

_LOGGER = logger.get(__name__)
_RAND: random.Random = random.Random(123)


class SimpleLocalSpannerMutex(spanner_mutex.SpannerMutex):
    def __init__(
        self,
        *,
        config: mutex.MutexConfig,
        client_uuid: Optional[uuid.UUID] = None,
        client_display_name: Optional[str] = None,
        target_filename: Optional[pathlib.Path] = None,
    ):
        super(SimpleLocalSpannerMutex, self).__init__(
            config=config, client_uuid=client_uuid, client_display_name=client_display_name
        )
        self._target_filename: pathlib.Path = preprocess.validate_type(  # type: ignore
            target_filename,
            "target_filename",
            pathlib.Path,
            is_none_valid=True,
            default_value=temp_filename(),
        )

    @property
    def target_filename(self) -> pathlib.Path:
        """
        Where the result of the critical section is.
        """
        return self._target_filename

    def is_mutex_needed(self) -> bool:
        return not self._target_filename.exists()

    def execute_critical_section(self, max_end_time: datetime) -> None:
        with open(self._target_filename, "a") as out_file:
            out_file.write(f"{self.client_display_name} - {self.client_uuid}\n")


def simple_mutex(
    config_filename: pathlib.Path, target_filename: Optional[pathlib.Path] = None
) -> SimpleLocalSpannerMutex:
    return _simple_mutex_instance(config=config_obj(config_filename), target_filename=target_filename)


def config_obj(config_filename: pathlib.Path) -> mutex.MutexConfig:
    with open(config_filename) as in_file:
        result = mutex.MutexConfig.from_json(in_file.read())
    return result


def _simple_mutex_instance(
    config: mutex.MutexConfig, target_filename: Optional[pathlib.Path] = None
) -> SimpleLocalSpannerMutex:
    if target_filename is None:
        target_filename = temp_filename()
    display_name = f"{SimpleLocalSpannerMutex.__name__}-{os.getpid()}-{threading.get_native_id()}"
    return SimpleLocalSpannerMutex(
        config=config,
        target_filename=target_filename,
        client_display_name=display_name,
    )


def temp_filename() -> pathlib.Path:
    return pathlib.Path(tempfile.NamedTemporaryFile().name)


def create_and_start_simple_mutex_client(
    config: mutex.MutexConfig, target_filename: Optional[pathlib.Path] = None
) -> None:
    instance = _simple_mutex_instance(config=config, target_filename=target_filename)
    time.sleep(_RAND.randint(0, 2))
    instance.start()


def threaded_multi_client(max_workers: int, func: Callable, *args, **kwargs) -> None:
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_clients = [executor.submit(func, *args, **kwargs) for _ in range(max_workers)]
        _ = concurrent.futures.wait(future_clients)

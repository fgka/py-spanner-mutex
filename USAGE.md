# How to integrate into your code

The basic concept is the following, based on code in [SimpleLocalSpannerMutex](code/src/py_spanner_mutex/_main_import.py):

```python
from datetime import datetime
from typing import Optional
import uuid


from py_spanner_mutex import spanner_mutex
from py_spanner_mutex.dto import mutex

# some code

class MySpannerMutex(spanner_mutex.SpannerMutex):
    def __init__(
        self,
        *,
        config: mutex.MutexConfig,
        client_uuid: Optional[uuid.UUID] = None,
        client_display_name: Optional[str] = None,
    ):
        super(MySpannerMutex, self).__init__(
            config=config, client_uuid=client_uuid, client_display_name=client_display_name
        )


    def is_mutex_needed(self) -> bool:
        result = False
        # Some code to check if the mutex is needed
        return result

    def execute_critical_section(self, max_end_time: datetime) -> None:
        # The code that needs to be executed exactly once
        return None

# some more code

def do_it(my_mutex: MySpannerMutex) -> None:
    # some more code
    my_mutex.start()
```

That is it!

## Caveats -- or the devil is in the details

First, we cannot make sure you properly implemented these two methods.
Second, if ``is_mutex_needed()`` fails, the entire mutex execution fails.
Third, if ``execute_critical_section()`` fails, it is assumed that there was an intermittent error.
It will keep trying until it (or another client) succeeds or max retries is reached.

### ``is_mutex_needed()``

There are many ways to cheat here, do not do it.
The simplest way to cheat is to get the current state and check if it is ``DONE``.
This is *absolutely* wrong and very unsafe.

Let us pick an example to illustrate how to properly implement ``is_mutex_needed()``.
We will assume there is a list of unique files with all database migrations that need to happen.
We also have a table that tracks the applied migrations so far.

Given the above, a sensible ``is_mutex_needed()`` will check the _local_ list against the change tracking table.
It will only return ``True`` if and only if the there are _new_ local files that were not applied yet, according to the tracking table.

### ``execute_critical_section()``

Once you are here, your code instance is guaranteed to be the only one being executed.
Using the example above, a good ``execute_critical_section()`` will take the migration _todo list_ and apply one by one, adding them -- as you go -- to the tracking table.
This will accomplish two things:
- You are only applying the changes that are needed.
- You have a resumable critical section, i.e., if your code crashes another instance can pick it up where you left.

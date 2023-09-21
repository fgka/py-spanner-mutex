# Using The CLI To Test It All

version: 0.1.0

> :hand: *ALL* commands are assumed to be executed from this folder: `./code`

## (Only Once) Prepare

Install cycle:

```bash
poetry install
```

Check if you have the config file:

```bash
CFG_FILE="../test_config.json"
[ -f ${CFG_FILE} ] && echo "All good, you have ${CFG_FILE}" || echo "Deploy spanner resources, you do not have ${CFG_FILE}"
```

## Very Simple, Single Client

> :hand: It is assumed you already have the infrastructure in [terraform](../terraform) and you have the file ``../test_config.json``

Simple test:

```bash
poetry run python -m py_spanner_mutex single-client --config-file ../test_config.json
```

Expected output:

```text
[2023-09-21 17:14:17,375 __main__.py.single_client:31]INFO: Starting single client critical section using: '{'config_file': PosixPath('../test_config.json')}'
INFO:__main__:Starting single client critical section using: '{'config_file': PosixPath(''../test_config.json')}'

...

[2023-09-21 17:14:21,568 __main__.py.single_client:34]INFO: Ended critical section for 'SimpleLocalSpannerMutex(config='MutexConfig(mutex_uuid=UUID('<UUID>'), instance_id='<INSTANCE ID>', database_id='distributed_mutex', table_id='distributed_mutex', project_id='<PROJECT ID>', mutex_display_name='test_mutex', mutex_ttl_in_secs=30, mutex_staleness_in_secs=300, mutex_wait_time_in_secs=20, mutex_max_retries=5)', client_uuid='<UUID>', client_display_name='SimpleLocalSpannerMutex-<PID>-<THREAD ID>', creds='None')' and result in '<temp file name>'
```

## More Abusive Test

> :hand: It is assumed you already have the infrastructure in [terraform](../terraform) and you have the file ``../test_config.json``

This test will create as many processes as there are cores in your machine.
It will also create 10 threads per process, each trying to execute the critical section.

Run the test:

```bash
poetry run python -m py_spanner_mutex multi-client --config-file ../test_config.json
```

Expected output:

```text
[2023-09-21 17:19:33,212 __main__.py.multi_client:59]INFO: Starting threaded multiple clients critical section using: '{'config_file': PosixPath('../test_config.json'), 'proc': 8, 'client_proc': 10}'
INFO:__main__:Starting threaded multiple clients critical section using: '{'config_file': PosixPath('../test_config.json'), 'proc': 8, 'client_proc': 10}'

...

[2023-09-21 17:19:40,624 spanner_mutex.py.start:197]INFO: Critical section execution = True and ended after 0 retries and 5.918082 seconds. Client: SimpleLocalSpannerMutex(config='MutexConfig(mutex_uuid=UUID('<UUID>'), instance_id='<INSTANCE ID>', database_id='distributed_mutex', table_id='distributed_mutex', project_id='<PROJECT ID>', mutex_display_name='test_mutex', mutex_ttl_in_secs=30, mutex_staleness_in_secs=300, mutex_wait_time_in_secs=20, mutex_max_retries=5)', client_uuid='<UUID>', client_display_name='SimpleLocalSpannerMutex-<PID>-<THREAD ID>', creds='None')

...

[2023-09-21 17:20:19,739 spanner_mutex.py.start:197]INFO: Critical section execution = False and ended after 1 retries and 44.023138 seconds. Client: SimpleLocalSpannerMutex(config='MutexConfig(mutex_uuid=UUID('<UUID>'), instance_id='<INSTANCE ID>', database_id='distributed_mutex', table_id='distributed_mutex', project_id='<PROJECT ID>', mutex_display_name='test_mutex', mutex_ttl_in_secs=30, mutex_staleness_in_secs=300, mutex_wait_time_in_secs=20, mutex_max_retries=5)', client_uuid='<UUID>', client_display_name='SimpleLocalSpannerMutex-<PID>-<THREAD ID>', creds='None')

...


[2023-09-21 17:20:34,258 __main__.py.multi_client:72]INFO: Ended critical section for 80 clients and result in '<temp file name>'
```

If you visualize the temp file in the last log line you should see a single line with the only client that was able to execute the critical section.
For instance:

```text
SimpleLocalSpannerMutex-60191-6157851 - 57d8dcfb-5152-4db9-8e35-a63c47e5e585
```

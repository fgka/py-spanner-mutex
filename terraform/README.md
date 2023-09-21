# Using Terraform to deploy all

> :hand: *ALL* commands are assumed to be executed from this folder: `./terraform`

## Authenticate (only once)

```bash
gcloud auth application-default login
```

### Set default project (only once)

```bash
gcloud init
```

## Definitions (only once)

Manually set:

```bash
export PROJECT_ID=$(gcloud config get-value core/project)
export REGION="europe-west3"
```

Because macOS does not adopt gnu-sed:

```bash
export SED="sed"
if [[ "Darwin" == $(uname -s) ]]; then
  export SED="gsed"
fi
echo "sed = '${SED}'"
```

## [Bootstrap](./bootstrap/README.md)

Only once, since it enable necessary APIs and creates the terraform state bucket.

## [Spanner Instance](./spanner_instance/README.md)

Will create a minimal Spanner instance.

## [Spanner Mutex](./spanner_mutex/README.md)

This creates the Spanner database into the available instance.
On the database it will create the mutex table.
The last bit is to generate the ``test_config.json`` to be used in [CLI](../code/CLI.md) testing.

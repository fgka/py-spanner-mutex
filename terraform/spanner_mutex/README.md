# Using Terraform to deploy all

It is assumed you ran the [bootstrap](../bootstrap/README.md) and [spanner_instance](../spanner_instance/README.md) instructions first.

## Definitions (only once)

Manually set:

```bash
export PROJECT_ID=$(gcloud config get-value core/project)
export REGION="europe-west3"
```

Either get the spanner instance from [spanner_instance](../spanner_instance/README.md) or set it manually:

```bash
export SPANNER_INST_NAME="YOUR_SPANNER_INSTANCE_NAME"
```

## Create ``terraform.tfvars`` (only once)

Because macOS does not adopt gnu-sed:

```bash
export SED="sed"
if [[ "Darwin" == $(uname -s) ]]; then
  export SED="gsed"
fi
echo "sed = '${SED}'"
```

Create:

```bash
cp -f terraform.tfvars.tmpl terraform.tfvars

${SED} -i \
  -e "s/@@PROJECT_ID@@/${PROJECT_ID}/g" \
  -e "s/@@REGION@@/${REGION}/g" \
  -e "s/@@SPANNER_INST_NAME@@/${SPANNER_INST_NAME}/g" \
  terraform.tfvars
```

Check:

```bash
cat terraform.tfvars
```

## Init

```bash
terraform init -upgrade
```

## Plan

```bash
TMP=$(mktemp)
terraform plan \
  -out ${TMP} \
  -var-file=terraform.tfvars
```

## Apply

```bash
terraform apply ${TMP} && rm -f ${TMP}
```

<!-- BEGINNING OF PRE-COMMIT-TERRAFORM DOCS HOOK -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.2.5 |
| <a name="requirement_google"></a> [google](#requirement\_google) | >= 4.44.0 |
| <a name="requirement_google-beta"></a> [google-beta](#requirement\_google-beta) | >= 4.44.0 |
| <a name="requirement_local"></a> [local](#requirement\_local) | >= 1.2.5 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_google"></a> [google](#provider\_google) | 4.83.0 |
| <a name="provider_local"></a> [local](#provider\_local) | 2.4.0 |
| <a name="provider_random"></a> [random](#provider\_random) | 3.5.1 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [google_spanner_database.database](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/spanner_database) | resource |
| [local_file.test_config_json](https://registry.terraform.io/providers/hashicorp/local/latest/docs/resources/file) | resource |
| [random_uuid.test_mutex](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/uuid) | resource |
| [google_spanner_instance.instance](https://registry.terraform.io/providers/hashicorp/google/latest/docs/data-sources/spanner_instance) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_is_production"></a> [is\_production](#input\_is\_production) | If it is production environment, then project the database and table | `bool` | `true` | no |
| <a name="input_max_mutex_table_row_ttl_in_days"></a> [max\_mutex\_table\_row\_ttl\_in\_days](#input\_max\_mutex\_table\_row\_ttl\_in\_days) | Once a mutex row in created, for how long to keep it before auto-removing it, in days | `number` | `3` | no |
| <a name="input_mutex_db_ddl_tmpl"></a> [mutex\_db\_ddl\_tmpl](#input\_mutex\_db\_ddl\_tmpl) | Spanner mutex DB DDL script template | `string` | `"templates/mutex_db_ddl.sql.tmpl"` | no |
| <a name="input_mutex_db_name"></a> [mutex\_db\_name](#input\_mutex\_db\_name) | Spanner database name for distributed mutex. | `string` | `"distributed_mutex"` | no |
| <a name="input_mutex_db_retention_in_hours"></a> [mutex\_db\_retention\_in\_hours](#input\_mutex\_db\_retention\_in\_hours) | For how long (in hours) to keep a Mutex database version | `number` | `72` | no |
| <a name="input_mutex_table_name"></a> [mutex\_table\_name](#input\_mutex\_table\_name) | Spanner table name distributed mutex. | `string` | `"distributed_mutex"` | no |
| <a name="input_project_id"></a> [project\_id](#input\_project\_id) | Project ID where to deploy and source of data. | `string` | n/a | yes |
| <a name="input_region"></a> [region](#input\_region) | Default region where to create resources. | `string` | `"us-central1"` | no |
| <a name="input_spanner_instance_name"></a> [spanner\_instance\_name](#input\_spanner\_instance\_name) | Spanner instance for distributed mutex. | `string` | n/a | yes |
| <a name="input_test_config_json"></a> [test\_config\_json](#input\_test\_config\_json) | Spanner mutex config file output file path relative to module path | `string` | `"../../test_config.json"` | no |
| <a name="input_test_config_json_tmpl"></a> [test\_config\_json\_tmpl](#input\_test\_config\_json\_tmpl) | Spanner mutex config file template to be used when testing the code | `string` | `"templates/test_config.json.tmpl"` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_spanner_database"></a> [spanner\_database](#output\_spanner\_database) | n/a |
| <a name="output_test_config_json"></a> [test\_config\_json](#output\_test\_config\_json) | n/a |
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->

# Using Terraform to deploy all

It is assumed you ran the [bootstrap](../bootstrap/README.md) instructions first.

## Definitions (only once)

Manually set:

```bash
export PROJECT_ID=$(gcloud config get-value core/project)
export REGION="europe-west3"
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

## Export Cloud Spanner instance name

```bash
OUT_JSON=$(mktemp)
terraform output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

export SPANNER_INST_NAME=$(jq -c -r ".spanner_instance.value.name" ${OUT_JSON})
echo "Terraform Spanner Instance name: '${SPANNER_INST_NAME}'"
rm -f ${OUT_JSON}
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
| <a name="provider_google"></a> [google](#provider\_google) | 4.82.0 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [google_spanner_instance.instance](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/spanner_instance) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_project_id"></a> [project\_id](#input\_project\_id) | Project ID where to deploy and source of data. | `string` | n/a | yes |
| <a name="input_region"></a> [region](#input\_region) | Default region where to create resources. | `string` | `"us-central1"` | no |
| <a name="input_spanner_config"></a> [spanner\_config](#input\_spanner\_config) | Spanner instance availability setting. | `string` | `null` | no |
| <a name="input_spanner_instance_name"></a> [spanner\_instance\_name](#input\_spanner\_instance\_name) | Spanner instance for distributed mutex. | `string` | `"Distributed mutex"` | no |
| <a name="input_spanner_processing_units"></a> [spanner\_processing\_units](#input\_spanner\_processing\_units) | Spanner instance processing units. | `number` | `100` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_spanner_instance"></a> [spanner\_instance](#output\_spanner\_instance) | n/a |
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->

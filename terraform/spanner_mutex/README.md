# Using Terraform to deploy all

It is assumed you ran the [bootstrap](../bootstrap/README.md) instructions first.

## Definitions (only once)

Manually set:

```bash
export PROJECT_ID=$(gcloud config get-value core/project)
export REGION="europe-west3"
```

Please set them properly:

```bash
export NOTIFICATION_EMAIL="${USER}@$(uname -n)"
export GITHUB_OWNER="${USER}"

export GITHUB_REPO=$(basename `git rev-parse --show-toplevel`)
export GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
```

Packages and versions:

```bash
YAAS_PIP_PACKAGES="["
unset PKGS
PKGS=("core" "service")
for P in ${PKGS[@]}; do
  pushd ../../code/${P}
  poetry version --no-ansi | read PKG_NAME PKG_VERSION
  YAAS_PIP_PACKAGES+="\"${PKG_NAME}>=${PKG_VERSION}\","
  popd
done
YAAS_PIP_PACKAGES=${YAAS_PIP_PACKAGES%%,}
YAAS_PIP_PACKAGES+="]"
export YAAS_PIP_PACKAGES=${YAAS_PIP_PACKAGES}
```

Calendar ID:

```bash
export CALENDAR_ID="YOUR_GOOGLE_CALENDAR_ID"
```

Check:

```bash
echo "Main project: ${PROJECT_ID}@${REGION}"
echo "Email: ${NOTIFICATION_EMAIL}"
echo "Github: ${GITHUB_OWNER}@${GITHUB_REPO}:${GIT_BRANCH}"
echo "YAAS python packages: ${YAAS_PIP_PACKAGES}"
echo "Google Calendar ID: ${CALENDAR_ID}"
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
  -e "s/@@TF_STATE_BUCKET@@/${TF_STATE_BUCKET}/g" \
  -e "s/@@NOTIFICATION_EMAIL@@/${NOTIFICATION_EMAIL}/g" \
  -e "s/@@GITHUB_OWNER@@/${GITHUB_OWNER}/g" \
  -e "s/@@GITHUB_REPO@@/${GITHUB_REPO}/g" \
  -e "s/@@GIT_BRANCH@@/${GIT_BRANCH}/g" \
  -e "s/@@CALENDAR_ID@@/${CALENDAR_ID}/g" \
  -e "s/@@GMAIL_USERNAME@@/${GMAIL_USERNAME}/g" \
  -e "s/@@YAAS_PIP_PACKAGE@@/${YAAS_PIP_PACKAGES}/g" \
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

## Trigger Build

Get trigger name:

```bash
OUT_JSON=$(mktemp)
terraform output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

CICD_TF_TRIGGER_NAME=$(jq -c -r ".cicd_build.value.tf_build_trigger.name" ${OUT_JSON})
echo "CI/CD Terraform trigger name: '${CICD_TF_TRIGGER_NAME}'"

rm -f ${OUT_JSON}
```

Trigger build:

```bash
TMP=$(mktemp)
gcloud builds triggers run ${CICD_TF_TRIGGER_NAME} \
  --branch=${GIT_BRANCH} \
  --region=${REGION} \
  --format=json \
  > ${TMP}

BUILD_ID=$(jq -r -c ".metadata.build.id" ${TMP})
echo "Build ID: '${BUILD_ID}'"

rm -f ${TMP}
```

Stream logs:

```bash
gcloud builds log ${BUILD_ID} --region=${REGION} --stream
```

Status:

```bash
gcloud builds describe ${BUILD_ID} --region=${REGION}
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

No providers.

## Modules

No modules.

## Resources

No resources.

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_project_id"></a> [project\_id](#input\_project\_id) | Project ID where to deploy and source of data. | `string` | n/a | yes |
| <a name="input_region"></a> [region](#input\_region) | Default region where to create resources. | `string` | `"us-central1"` | no |

## Outputs

No outputs.
<!-- END OF PRE-COMMIT-TERRAFORM DOCS HOOK -->

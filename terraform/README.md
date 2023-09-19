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

**NOTE:** You should need this only once.

```bash
export TF_DIR="./bootstrap"
```

### Create ``terraform.tfvars`` (only once)

```bash
cp -f ${TF_DIR}/terraform.tfvars.tmpl ${TF_DIR}/terraform.tfvars

${SED} -i \
  -e "s/@@PROJECT_ID@@/${PROJECT_ID}/g" \
  -e "s/@@REGION@@/${REGION}/g" \
  ${TF_DIR}/terraform.tfvars
```

### Init

```bash
terraform -chdir=${TF_DIR} init -upgrade
```

### Plan

```bash
TMP=$(mktemp)
terraform -chdir=${TF_DIR} plan \
  -out ${TMP} \
  -var-file=terraform.tfvars
```

### Apply

```bash
terraform -chdir=${TF_DIR} apply ${TMP} && rm -f ${TMP}
```

### Export bucket name

```bash
OUT_JSON=$(mktemp)
terraform -chdir=${TF_DIR} output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

export TF_STATE_BUCKET=$(jq -c -r ".tf_state_bucket.value.name" ${OUT_JSON})
echo "Terraform state bucket name: '${TF_STATE_BUCKET}'"
rm -f ${OUT_JSON}
```

### Copy generated `backend.tf` over to each module

```bash
TARGET_FILENAME="backend.tf"
OUT_JSON=$(mktemp)
terraform -chdir=${TF_DIR} output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

jq -c -r ".backend_tf.value[]" ${OUT_JSON} \
  | while read FILENAME; \
    do \
      ACTUAL_FILENAME="${TF_DIR}/${FILENAME}"
      MODULE=${FILENAME##*.}; \
      OUTPUT="./${MODULE}/${TARGET_FILENAME}"; \
      echo "Copying: '${ACTUAL_FILENAME}' to '${OUTPUT}'"; \
      cp ${ACTUAL_FILENAME} ${OUTPUT}; \
    done
rm -f ${OUT_JSON}
```

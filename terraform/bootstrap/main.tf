////////////////////
// Global/General //
////////////////////

locals {
  # root dir
  terraform_module_root_dir = path.module
  output_dir                = "${local.terraform_module_root_dir}/output"
  # backend.tf
  backend_tf_tmpl = "${local.terraform_module_root_dir}/${var.backend_tf_tmpl}"
  backend_tf      = "${local.output_dir}/${var.backend_tf}"
  # output
  output_filenames = [
    for local_filename in local_file.backend_tf : local_filename.filename
  ]
}

data "google_project" "project" {
  project_id = var.project_id
}


//////////
// APIs //
//////////

resource "google_project_service" "project" {
  for_each = toset(var.minimum_apis)
  project  = var.project_id
  service  = each.key
  timeouts {
    create = "30m"
    update = "40m"
  }
  disable_dependent_services = true
  disable_on_destroy         = true
}

/////////////
// Buckets //
/////////////

module "tf_state_bucket" { #tfsec:ignore:google-storage-bucket-encryption-customer-key
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/gcs"
  project_id = var.project_id
  prefix     = var.tf_state_bucket_name_prefix
  name       = data.google_project.project.number
}

////////////////
// backend.tf //
////////////////

resource "local_file" "backend_tf" {
  for_each = toset(var.backend_tf_modules)
  content = templatefile(local.backend_tf_tmpl, {
    BUCKET_NAME = module.tf_state_bucket.bucket.name,
    REGION      = var.region,
    MODULE      = each.key
  })
  filename = "${local.backend_tf}.${each.key}"
}

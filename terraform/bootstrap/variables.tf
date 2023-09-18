////////////////////
// Global/General //
////////////////////

variable "project_id" {
  description = "Project ID where to deploy and source of data."
  type        = string
}

variable "region" {
  description = "Default region where to create resources."
  type        = string
  default     = "us-central1"
}

//////////
// APIs //
//////////

variable "minimum_apis" {
  description = "Minimum APIs to activate in the project. Only change if you know what you are doing."
  type        = list(string)
  default = [
    "cloudapis.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "run.googleapis.com",
    "servicemanagement.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
  ]
}

/////////////
// Buckets //
/////////////

variable "tf_state_bucket_name_prefix" {
  description = "Prefix to name the terraform state bucket, the suffix is the project numerical ID."
  type        = string
  default     = "spanner-mutex-tf"
}

////////////////
// backend.tf //
////////////////

variable "backend_tf_modules" {
  description = "Modules with their own Terraform state. Only change if you know what you are doing."
  type        = list(string)
  default = [
    "spanner_mutex",
  ]
}

variable "backend_tf_tmpl" {
  description = "Template for backend.tf. Only change if you know what you are doing."
  type        = string
  default     = "templates/backend.tf.tmpl"
}

variable "backend_tf" {
  description = "Where to store backend.tf. Only change if you know what you are doing."
  type        = string
  default     = "backend.tf"
}

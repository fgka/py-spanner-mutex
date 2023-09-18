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

//////////////////////
// Spanner Instance //
//////////////////////

variable "spanner_config" {
  description = "Spanner instance availability setting."
  type        = string
  default     = null
}

variable "spanner_instance_name" {
  description = "Spanner instance for distributed mutex."
  type        = string
  default     = "Distributed mutex"
}

variable "spanner_processing_units" {
  description = "Spanner instance processing units."
  type        = number
  default     = 100
}

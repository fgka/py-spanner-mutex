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

variable "spanner_instance_name" {
  description = "Spanner instance for distributed mutex."
  type        = string
}

////////////////////////////
// Spanner Mutex Database //
////////////////////////////

variable "mutex_db_name" {
  description = "Spanner database name for distributed mutex."
  type        = string
  default     = "distributed_mutex"
}

variable "mutex_db_retention_in_hours" {
  description = "For how long (in hours) to keep a Mutex database version"
  type        = number
  default     = 72
}

///////////////////////
// Spanner Mutex DDL //
///////////////////////

variable "mutex_db_ddl_tmpl" {
  description = "Spanner mutex DB DDL script template"
  type        = string
  default     = "templates/mutex_db_ddl.sql.tmpl"
}

variable "mutex_table_name" {
  description = "Spanner table name distributed mutex."
  type        = string
  default     = "distributed_mutex"
}

variable "max_mutex_table_row_ttl_in_days" {
  description = "Once a mutex row in created, for how long to keep it before auto-removing it, in days"
  type        = number
  default     = 3
}

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

variable "is_production" {
  description = "If it is production environment, then project the database and table"
  type        = bool
  default     = true
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

variable "test_config_json_tmpl" {
  description = "Spanner mutex config file template to be used when testing the code"
  type        = string
  default     = "templates/test_config.json.tmpl"
}

variable "test_config_json" {
  description = "Spanner mutex config file output file path relative to module path"
  type        = string
  default     = "../../test_config.json"
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

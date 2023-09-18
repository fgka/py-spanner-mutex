////////////////////
// Global/General //
////////////////////

locals {
  # templating
  terraform_module_root_dir = path.module
  mutex_db_ddl_tmpl         = "${local.terraform_module_root_dir}/${var.mutex_db_ddl_tmpl}"
  # DDL Template
  mutex_db_ddl_template_values = {
    MUTEX_TABLE_NAME      = var.mutex_table_name,
    MUTEX_ROW_TTL_IN_DAYS = var.max_mutex_table_row_ttl_in_days,
  }
}

//////////////////////
// Spanner Instance //
//////////////////////

data "google_spanner_instance" "instance" {
  project = var.project_id
  name    = var.spanner_instance_name
}

////////////////////////////
// Spanner Mutex Database //
////////////////////////////

resource "google_spanner_database" "database" {
  instance                 = data.google_spanner_instance.instance.name
  name                     = var.mutex_db_name
  version_retention_period = "${var.mutex_db_retention_in_hours}h"
  ddl                      = [templatefile(local.mutex_db_ddl_tmpl, local.mutex_db_ddl_template_values)]
  database_dialect         = "GOOGLE_STANDARD_SQL"
  deletion_protection      = false
  enable_drop_protection   = true
}

////////////////////
// Global/General //
////////////////////

locals {
  # templating
  terraform_module_root_dir = path.module
  mutex_db_ddl_tmpl         = abspath("${local.terraform_module_root_dir}/${var.mutex_db_ddl_tmpl}")
  # DDL Template
  mutex_db_ddl_template_values = {
    MUTEX_TABLE_NAME      = var.mutex_table_name,
    MUTEX_ROW_TTL_IN_DAYS = var.max_mutex_table_row_ttl_in_days,
  }
  # test config JSON
  test_config_json_tmpl = abspath("${local.terraform_module_root_dir}/${var.test_config_json_tmpl}")
  test_config_json      = abspath("${local.terraform_module_root_dir}/${var.test_config_json}")
  test_config_template_values = {
    MUTEX_UUID        = random_uuid.test_mutex.result
    PROJECT_ID        = var.project_id
    MUTEX_INSTANCE_ID = data.google_spanner_instance.instance.name
    MUTEX_DATABASE_ID = google_spanner_database.database.name
    MUTEX_TABLE_ID    = var.mutex_table_name
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
  deletion_protection      = var.is_production
  enable_drop_protection   = var.is_production
}

//////////////////////
// Test Config JSON //
//////////////////////

resource "random_uuid" "test_mutex" {
}

resource "local_file" "test_config_json" {
  content  = templatefile(local.test_config_json_tmpl, local.test_config_template_values)
  filename = local.test_config_json
}

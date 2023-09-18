////////////////////
// Global/General //
////////////////////

locals {
  spanner_instance_config = coalesce(var.spanner_config, "regional-${var.region}")
}

//////////////////////
// Spanner Instance //
//////////////////////

resource "google_spanner_instance" "instance" {
  project          = var.project_id
  config           = local.spanner_instance_config
  display_name     = var.spanner_instance_name
  processing_units = var.spanner_processing_units
}

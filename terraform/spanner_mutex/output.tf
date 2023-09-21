////////////////////////////
// Spanner Mutex Database //
////////////////////////////

output "spanner_database" {
  value = google_spanner_database.database
}

output "test_config_json" {
  value = local_file.test_config_json.filename
}

////////////////////////////
// Spanner Mutex Database //
////////////////////////////

output "spanner_database" {
  value = google_spanner_database.database
}

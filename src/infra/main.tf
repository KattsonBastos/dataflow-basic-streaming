resource "google_storage_bucket" "practicing_beam" {
  name = "practicing-beam"
  location = var.region
  project = var.project_id
  force_destroy = true
}

resource "google_bigquery_dataset" "dataset_website" {
  dataset_id    = "website"
  friendly_name = "website"
  description   = "Data Platform - Website DW"
  location      = var.region
  project = var.project_id
}

# resource "google_bigquery_table" "table_user" {
#   dataset_id  = google_bigquery_dataset.dataset_website.dataset_id
#   table_id    = "user"
#   description = "Table for user data"
#   deletion_protection=false 
#   project = var.project_id
# }

resource "google_pubsub_topic" "user_input" {
  name = "user-input"
  project = var.project_id
}

resource "google_pubsub_subscription" "user_input_sub" {
  name    = "${google_pubsub_topic.user_input.name}-sub"
  topic   = google_pubsub_topic.user_input.name
  project = var.project_id
}
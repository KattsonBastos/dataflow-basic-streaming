provider "google" {
  region  = var.region
  credentials = file("/tmp/beam-key.json")
}

terraform {
  backend "gcs" {
    bucket = "beam-tf-state-dev"
    prefix = "terraform/state"
    credentials = "/tmp/beam-key.json"
  }
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.67.0"
    }
  }
}
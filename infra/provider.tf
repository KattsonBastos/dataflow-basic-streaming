terraform {
    required_providers {
        google = {
            source = "hashicorp/google"
        }
    }
}

provider "google" {
  project = "k-practices"
  region  = "us-east1"
}
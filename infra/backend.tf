terraform {
    backend "gcs" {
        bucket = "tf-state-dev"
        prefix = "terraform/state"
        credentials = "/tmp/beam-key.json"
    }
}
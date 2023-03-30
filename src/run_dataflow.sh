#!/bin/bash

streaming() {
  echo "### Creating Streaming Dataflow Job.."

  gcloud dataflow jobs run user_data --gcs-location gs://practicing-beam/template/streaming_user_bq \
   --region us-east1 --staging-location gs://practicing-beam/temp/

  echo "### Streaming Dataflow Job Created!"
 
}

case $1 in
  streaming)
    streaming
    ;;
  *)
    echo "Usage: $0 {streaming}"
    ;;
esac
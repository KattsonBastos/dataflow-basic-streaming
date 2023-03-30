import os
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# local modules
from constants import Constants as run_constants


# START: setting GS credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = run_constants.serviceAccount.value
# END: setting GS credentials

# START: defining helper functions
def calculate_trip_time(record):
    from datetime import datetime # importing here otherwise DataFlow workers won't find it 
    ## ```NameError: name 'datetime' is not defined```
    ## I'm still trying to solve it

    pickup_datetime = datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S')
    dropoff_datetime = datetime.strptime(record[2], '%Y-%m-%d %H:%M:%S')
    
    return (record[1].split(' ')[0], float((dropoff_datetime - pickup_datetime).seconds))
## END: defining helper functions

## START: defining main function
def run():
  pipeline_options = {
      'project': 'k-practices' ,
      'runner': 'DataflowRunner',
      'region': 'us-east1',
      'staging_location': 'gs://practicing-beam/temp',
      'temp_location': 'gs://practicing-beam/temp',
      'template_location': 'gs://practicing-beam/template/batch_job_df_gcs_taxi',
      'save_main_session' : True 
  }

  # defining the pipeline
  pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
  p1 = beam.Pipeline(options=pipeline_options)

  miles = (
  p1
    | "Loading Data for Miles" >> beam.io.ReadFromText(r"gs://practicing-beam/input/yellow_tripdata_2023-01.csv", skip_header_lines = 1)
    | "Separating by Comma for Miles" >> beam.Map(lambda record: record.split(','))
    | "Create pair value for Miles" >> beam.Map(lambda record: (record[1].split(' ')[0], float(record[4])))
    | "Elapsed Miles by Day for Miles" >> beam.CombinePerKey(sum)
  #   | "Saving Results for Miles" >> beam.io.WriteToText("../data/miles.txt")
  )


  fare_amount = (
  p1
    | "Loading Data" >> beam.io.ReadFromText(r"gs://practicing-beam/input/yellow_tripdata_2023-01.csv", skip_header_lines = 1)
    | "Separating by Comma" >> beam.Map(lambda record: record.split(','))
    | "Create pair value" >> beam.Map(lambda record: (record[1].split(' ')[0], float(record[10])))
    | "Fare Amount by Day" >> beam.CombinePerKey(sum)
  #   | "Saving Results" >> beam.io.WriteToText("../data/fare.txt")
  )

  total_time = (
  p1
    | "Loading Data Total Time" >> beam.io.ReadFromText(r"gs://practicing-beam/input/yellow_tripdata_2023-01.csv", skip_header_lines = 1)
    | "Separating by Comma Total Time" >> beam.Map(lambda record: record.split(','))
    | "Create pair value Total Time" >> beam.Map(calculate_trip_time)
    | "Total Time by Day" >> beam.CombinePerKey(sum)
  #   | "Saving Results" >> beam.io.WriteToText("../data/time.txt")
  )

  joining = (
      {
          'miles':miles,
          'fare_amount':fare_amount,
          'total_time': total_time
      }
      | "Group By" >> beam.CoGroupByKey()
      | "Saving Results" >> beam.io.WriteToText(r"gs://practicing-beam/output/results.csv")
  )

  # executing the entire pipeline
  p1.run()
# END: defining main function


if __name__ == '__main__':
  run()
# gcloud dataflow jobs run third-run-taxi --gcs-location gs://practicing-beam/template/batch_job_df_gcs_taxi --region us-east1 --staging-location gs://practicing-beam/temp/ 
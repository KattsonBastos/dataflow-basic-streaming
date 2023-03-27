import os
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# local modules
from constants import Constants as run_constants

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = run_constants.serviceAccount.value


def run():
    pipeline_options = {
        'project': 'k-practices',
        'runner': 'DataflowRunner',
        'region': 'us-east1',
        'staging_location': 'gs://practicing-beam/temp',
        'temp_location': 'gs://practicing-beam/temp',
        'template_location': 'gs://practicing-beam/template/batch_job_df_bq_taxi',
        'save_main_session': True
    }

    # defining the pipeline
    pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
    p1 = beam.Pipeline(options=pipeline_options)


    def calculate_trip_time(record):
        from datetime import datetime  # importing here otherwise DataFlow workers won't find it 
        # ```NameError: name 'datetime' is not defined```

        pickup_datetime = datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S')
        dropoff_datetime = datetime.strptime(record[2], '%Y-%m-%d %H:%M:%S')

        return record[1].split(' ')[0], float((dropoff_datetime - pickup_datetime).seconds)


    def filter_month(record):
        from datetime import datetime  # importing here otherwise DataFlow workers won't find it 
        # ```NameError: name 'datetime' is not defined```

        month_number = record[0].split('-')[1]

        if month_number == '01':
            return True
            

    def create_dict(record):
        return {
            'day': record[0],
            'miles': record[1]['miles'][0],
            'fare_amount': record[1]['fare_amount'][0],
            'total_time': record[1]['total_time'][0]
        }

    table_schema = 'day:STRING, miles:FLOAT, fare_amount:FLOAT, total_time:FLOAT'
    table_name = 'k-practices:dataflow_taxi.taxi_data_per_day'

    miles = (
            p1
            | "Loading Data Miles" >> beam.io.ReadFromText(r"gs://practicing-beam/input/yellow_tripdata_2023-01.csv",
                                                           skip_header_lines=1)
            | "Separating by Comma Miles" >> beam.Map(lambda record: record.split(','))
            | "Create pair value Miles" >> beam.Map(lambda record: (record[1].split(' ')[0], float(record[4])))
            | "Filter Month Miles" >> beam.Filter(filter_month)
            | "Elapsed Miles by Day Miles" >> beam.CombinePerKey(sum)
        #   | "Saving Results for Miles" >> beam.io.WriteToText("../data/miles.txt")
    )

    fare_amount = (
            p1
            | "Loading Data Fare" >> beam.io.ReadFromText(r"gs://practicing-beam/input/yellow_tripdata_2023-01.csv",
                                                          skip_header_lines=1)
            | "Separating by Comma Fare" >> beam.Map(lambda record: record.split(','))
            | "Create pair value Fare" >> beam.Map(lambda record: (record[1].split(' ')[0], float(record[10])))
            | "Filter Month Fare" >> beam.Filter(filter_month)
            | "Fare Amount by Day Fare" >> beam.CombinePerKey(sum)
        #   | "Saving Results" >> beam.io.WriteToText("../data/fare.txt")
    )

    total_time = (
            p1
            | "Loading Data Total Time" >> beam.io.ReadFromText(r"gs://practicing-beam/input/yellow_tripdata_2023-01.csv", skip_header_lines=1)
            | "Separating by Comma Total Time" >> beam.Map(lambda record: record.split(','))
            | "Create pair value Total Time" >> beam.Map(calculate_trip_time)
            | "Filter Month Total Time" >> beam.Filter(filter_month)
            | "Total Time by Day" >> beam.CombinePerKey(sum)
        #   | "Saving Results" >> beam.io.WriteToText("../data/time.txt")
    )

    joining = (
            {
                'miles': miles,
                'fare_amount': fare_amount,
                'total_time': total_time
            }
            | beam.CoGroupByKey()
            | beam.Map(lambda record: create_dict(record))
            # | beam.Map(lambda record: desaninhar_dict(record))
            # | beam.Map(lambda record: criar_dict_nivel0(record)) 
            | beam.io.WriteToBigQuery(
        table_name,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        custom_gcs_temp_location='gs://practicing-beam/temp')

    )

    # executing the entire pipeline
    p1.run()


# gcloud dataflow jobs run second-run-taxi-bq --gcs-location gs://practicing-beam/template/batch_job_df_bq_taxi --region us-east1 --staging-location gs://practicing-beam/temp/ 

if __name__ == '__main__':
    run()
import os
from datetime import datetime


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# local modules
from constants import Constants as run_constants

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = run_constants.serviceAccount.value

file_name = r"{data_path}{dataset_name}".format(
    data_path=run_constants.data_input_path.value,
    dataset_name = 'yellow_tripdata_2023-01.csv')

pipeline_options = {
    'project': 'k-practices' ,
    'runner': 'DataflowRunner',
    'region': 'us-east1',
    'staging_location': 'gs://practicing-beam/temp',
    'temp_location': 'gs://practicing-beam/temp',
    'template_location': 'gs://practicing-beam/template/batch_job_df_bq_taxi',
    'save_main_session' : True 
}

# defining the pipeline
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)


def calculate_trip_time(record):

    pickup_datetime = datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S')
    dropoff_datetime = datetime.strptime(record[2], '%Y-%m-%d %H:%M:%S')
    
    return (dropoff_datetime - pickup_datetime).seconds

def criar_dict_nivel1(record):
    dict_ = {} 
    dict_['day'] = record[0]
    dict_['lista'] = record[1]
    return(dict_)

def desaninhar_dict(record):
    def expand(key, value):
        if isinstance(value, dict):
            return [ (key + '_' + k, v) for k, v in desaninhar_dict(value).items() ]
        else:
            return [ (key, value) ]
    items = [ item for k, v in record.items() for item in expand(k, v) ]
    return dict(items)


def criar_dict_nivel0(record):
    dict_ = {} 
    dict_['day'] = record['day']
    dict_['lista_miles'] = record['miles'][0]
    dict_['lista_fare_amount'] = record['fare_amount'][0]
    dict_['lista_total_time'] = record['total_time'][0]
    return(dict_)

table_schema = 'day:STRING, miles:FLOAT, fare_amount:FLOAT, total_time:FLOAT'
tabela = 'practicing-beam:dataflow_taxi.curso_dataflow_voos_atraso'


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
  | "Create pair value Total Time" >> beam.Map(lambda record: (record[1].split(' ')[0], float(calculate_trip_time(record))))
  | "Total Time by Day" >> beam.CombinePerKey(sum)
#   | "Saving Results" >> beam.io.WriteToText("../data/time.txt")
)

joining = (
    {
        'miles':miles,
        'fare_amount':fare_amount,
        'total_time': total_time
    }
    | beam.CoGroupByKey()
    | beam.Map(lambda record: criar_dict_nivel1(record))
    | beam.Map(lambda record: desaninhar_dict(record))
    | beam.Map(lambda record: criar_dict_nivel0(record)) 
    | beam.io.WriteToBigQuery(
                              tabela,
                              schema=table_schema,
                              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                              custom_gcs_temp_location = 'gs://practicing-beam/temp' )

)

# executing the entire pipeline
p1.run()
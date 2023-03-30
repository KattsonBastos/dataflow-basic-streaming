import os
import json

# beam-specific modules
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# local modules
from constants import Constants as run_constants


class Decode(beam.DoFn):
  def process(self,record):

    import json # importing here otherwise DataFlow workers won't find it 
    ## ```NameError: name 'json' is not defined```
    ## I'm still trying to solve it

    raw_data = record.decode("utf-8")

    raw_data = json.loads(raw_data)['results']

    records = [
        (
            data['login']['username'],
            data['name']['first'],
            data['name']['last'],
            data['gender'],
            data['dob']['date'],
            data['location']['postcode'],
            data['location']['country'],
            data['email']
        ) for data in raw_data
    ]
    
    return tuple(records)


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = run_constants.serviceAccount.value
subscription = 'projects/k-practices/subscriptions/user-input-sub'

def run():
    def create_dict(record):
        print(record)
        return {
            'login_username': record[0],
            'first_name': record[1],
            'last_name': record[2],
            'gender': record[3],
            'dob': record[4],
            'postcode': record[5],
            'country': record[6],
            'email': record[7]
        }

    table_schema = 'login_username:STRING, first_name:STRING, last_name:STRING, gender:STRING, dob:STRING, country:STRING, postcode:STRING, email:STRING'
    table_name = 'k-practices:website.users'

    pipeline_options = {
        'project': 'k-practices',
        'runner': 'DataflowRunner',
        'region': 'us-east1',
        'staging_location': 'gs://practicing-beam/temp',
        'temp_location': 'gs://practicing-beam/temp',
        'template_location': 'gs://practicing-beam/template/streaming_user_bq',
        'save_main_session': True,
        'streaming' : True
    }

    pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
    p1 = beam.Pipeline(options=pipeline_options)

    pcollection_entrada = (
        p1  | 'Read from pubsub topic' >> beam.io.ReadFromPubSub(subscription= subscription)
    )

    user_data = (
        pcollection_entrada
        | "Decode Json" >> beam.ParDo(Decode())
        | "Convert tuple to dict" >> beam.Map(lambda record: create_dict(record))
        | "Save to BigQuery" >> beam.io.WriteToBigQuery(
        table_name,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        custom_gcs_temp_location='gs://practicing-beam/temp')
    )

    # executing the entire pipeline
    result = p1.run()
    result.wait_until_finish()


if __name__ == '__main__':
  run()
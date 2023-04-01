from enum import Enum


class Constants(Enum):
    data_input_path = "gs://practcing-beam/input/"
    data_output_path = "gs://practcing-beam/output/"
    beam_staging_path = "gs://practcing-beam/temp/"
    beam_template_path = "gs://practcing-beam/template/"

    gcp_project_name = 'k-practices'
    gcp_user_input_topic = 'user-input'
    user_api_url = 'https://randomuser.me/api'

    serviceAccount = r'/tmp/beam-key.json'
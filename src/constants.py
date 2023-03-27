from enum import Enum


class Constants(Enum):
    data_input_path = "gs://practcing-beam/input/"
    data_output_path = "gs://practcing-beam/output/"

    serviceAccount = r'/tmp/beam-key.json'
import json
import numpy as np
import boto3
from botocore.config import Config


class WriteConfiguration:
    def __init__(self, bucket: str, date: str):
        my_config = Config(
            region_name='us-west-1',
            signature_version='v4',
            retries={
                'max_attempts': 10,
                'mode': 'standard'
            }
        )
        self.s3Client = boto3.client('s3', config=my_config)
        self.bucket = bucket
        self.date = date


def categories_to_s3(write_configuration: WriteConfiguration, categories: list):
    __write_json_to_s3(write_configuration, 'categories/categories', categories)


def stations_to_s3(write_configuration: WriteConfiguration, stations: list):
    __write_json_to_s3(write_configuration, 'stations/stations', stations)


def timeseries_to_s3(write_configuration: WriteConfiguration, timeseries: list):
    __write_json_to_s3(write_configuration, "timeseries", timeseries)


def timeseries_data_to_s3(write_configuration: WriteConfiguration, timeseries_data: dict):
    timeseries_data_list = []
    for key in timeseries_data.keys():
        value_this = timeseries_data.get(key)
        timeseries_data_list.append({"timeseriesdata_id": key, "values": value_this['values']})

    __write_json_to_s3(write_configuration, "timeseriesdata", timeseries_data_list)


def __write_json_to_s3(write_configuration: WriteConfiguration, id: str, body):
    json_lines = "\n".join(json.dumps(obj) for obj in body)
    __write_to_s3(write_configuration, id, json_lines)


def __write_to_s3(write_configuration: WriteConfiguration, id: str, json_lines):
    key = "alex-data/raw/" + write_configuration.date + "/" + str(id) + ".json"
    write_configuration.s3Client.put_object(Bucket=write_configuration.bucket, Key=key, Body=json_lines)
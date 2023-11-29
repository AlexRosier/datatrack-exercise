from datetime import datetime

import api_reader
import s3_writer
import station_smasher
import timeseries_smasher
import timeseries_data_smasher
import os


def handle_categories(write_configuration: s3_writer.WriteConfiguration):
    categories = api_reader.get_categories()
    s3_writer.categories_to_s3(write_configuration, categories)


def handle_stations(write_configuration: s3_writer.WriteConfiguration):
    stations = api_reader.get_stations()
    smashed_stations = station_smasher.smash(stations)
    s3_writer.stations_to_s3(write_configuration, smashed_stations)


def handle_timeseries(write_configuration: s3_writer.WriteConfiguration) -> list:
    timeseries = api_reader.get_timeseries()
    smashed_timeseries = timeseries_smasher.smash(timeseries)
    s3_writer.timeseries_to_s3(write_configuration, smashed_timeseries)
    return timeseries


def handle_timeseries_data(write_configuration: s3_writer.WriteConfiguration, timeseries: list):
    timeseries_data = api_reader.get_time_series_data(write_configuration.date, timeseries)
    smashed_timeseries_data = timeseries_data_smasher.smash(timeseries_data)
    s3_writer.timeseries_data_to_s3(write_configuration, smashed_timeseries_data)


def main():
    write_configuration = s3_writer.WriteConfiguration(os.getenv("bucket"), os.getenv("date"))

    handle_categories(write_configuration)
    handle_stations(write_configuration)
    handle_timeseries(write_configuration)
    handle_timeseries_data(write_configuration, api_reader.get_timeseries())
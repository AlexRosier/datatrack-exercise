import sys
import api_reader
import s3_writer
import os
import logging
import argparse


def handle_categories(write_configuration: s3_writer.WriteConfiguration):
    categories = api_reader.get_categories()
    s3_writer.categories_to_s3(write_configuration, categories)


def handle_stations(write_configuration: s3_writer.WriteConfiguration):
    stations = api_reader.get_stations()
    s3_writer.stations_to_s3(write_configuration, stations)


def handle_timeseries(write_configuration: s3_writer.WriteConfiguration) -> list:
    timeseries = api_reader.get_timeseries()
    s3_writer.timeseries_to_s3(write_configuration, timeseries)
    return timeseries


def handle_timeseries_data(write_configuration: s3_writer.WriteConfiguration, timeseries: list):
    timeseries_data = api_reader.get_time_series_data(write_configuration.date, timeseries)
    s3_writer.timeseries_data_to_s3(write_configuration, timeseries_data)


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.info("Entering ingest main")

    parser = argparse.ArgumentParser(description="Ingest")
    parser.add_argument(
        "-d", "--date", dest="date", help="Date in format YYYY-mm-dd", required=True
    )
    args = parser.parse_args()
    logging.info(f"Using args: {args}")

    write_configuration = s3_writer.WriteConfiguration(os.getenv("bucket"), args.date)

    handle_categories(write_configuration)
    handle_stations(write_configuration)
    timeseries = handle_timeseries(write_configuration)
    handle_timeseries_data(write_configuration, timeseries)


if __name__ == "__main__":
    main()
import requests
from datetime import datetime


def get_categories() -> list:
    categories_url = "https://geo.irceline.be/sos/api/v1/categories"
    return __get_json(categories_url)


def get_stations() -> list:
    stations_url = "https://geo.irceline.be/sos/api/v1/stations?expanded=true"
    return __get_json(stations_url)


def get_timeseries() -> list:
    timeseries_url = "https://geo.irceline.be/sos/api/v1/timeseries"
    return __get_json(timeseries_url)


def get_time_series_data(date_to_tech: str, timeseries: list) -> list:
    timeseries_ids = list(map(lambda timeserie: timeserie['id'], timeseries))
    timespan = __get_timespan(date_to_tech)

    timeseries_data_url = "https://geo.irceline.be/sos/api/v1/timeseries/getData"
    headers = {'Content-Type': 'application/json'}
    params = {
        "timespan": timespan,
        "timeseries": timeseries_ids
    }
    return requests.post(timeseries_data_url, json=params, headers=headers).json()


def __get_timespan(date_to_fetch: str) -> str:
    date_time = datetime.strptime(date_to_fetch, '%d-%m-%Y')
    return "PT24H/" + str(date_time.strftime('%Y-%m-%d')) + "TZ"


def __get_json(url: str) -> list:
    response = requests.get(url)
    return response.json()

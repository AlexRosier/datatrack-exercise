class SmashedTimeserieData:
    def __init__(self, timeseries_id: str, timedata: dict):
        self.timeseries_data_id = timeseries_id
        self.timeseries_data_timestamp = timedata['timestamp']
        self.timeseries_data_value = timedata['value']


def smash(timeseries_data: dict) -> list:
    smashed_timeseries_data = []

    for key in timeseries_data.keys():
        smashed_timeserie_datas = __smash_timeserie_data(key, timeseries_data[key]['values'])
        smashed_timeseries_data.extend(smashed_timeserie_datas)
    return smashed_timeseries_data


def __smash_timeserie_data(timeseries_data_id: str, timeserie_data_values: list) -> list:
    smashed_timeserie_datas = []
    for timeserie_data_value in timeserie_data_values:
        smashed_timeserie_datas.append(SmashedTimeserieData(timeseries_data_id, timeserie_data_value))
    return smashed_timeserie_datas

class SmashedTimeserie:
    def __init__(self, timeseries: dict):
        self.timeseries_id = timeseries['id']
        self.timeseries_label = timeseries['label']
        self.timeseries_uom = timeseries['uom']

        self.timeseries_station_id = timeseries['station']['properties']['id']
        self.timeseries_station_label = timeseries['station']['properties']['label']
        self.timeseries_station_type = timeseries['station']['type']

        station_geometry = timeseries['station']['geometry']
        self.station_coordinates_x = station_geometry['coordinates'][0]
        self.station_coordinates_y = station_geometry['coordinates'][1]
        self.station_coordinates_type = station_geometry['type']


def smash(timeseries: list) -> list:
    smashed_timeseries = []
    for timeserie in timeseries:
        smashed_timeserie = __smash_timeserie(timeserie)
        smashed_timeseries.append(smashed_timeserie)
    return smashed_timeseries


def __smash_timeserie(timeserie: dict) -> SmashedTimeserie:
    return SmashedTimeserie(timeserie)

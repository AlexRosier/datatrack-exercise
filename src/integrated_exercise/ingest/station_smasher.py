
class SmashedStation:
    def __init__(self, station: dict, timeseries_id: str, timeseries_properties: dict):
        properties = station['properties']
        self.station_id = properties['id']
        self.station_label = properties['label']
        self.station_type = station['type']

        self.timeseries_id = timeseries_id
        self.timeseries_services_id = timeseries_properties['service']['id']
        self.timeseries_services_label = timeseries_properties['service']['label']

        self.timeseries_offering_id = timeseries_properties['offering']['id']
        self.timeseries_offering_label = timeseries_properties['offering']['label']

        self.timeseries_feature_id = timeseries_properties['feature']['id']
        self.timeseries_feature_label = timeseries_properties['feature']['label']

        self.timeseries_procedure_id = timeseries_properties['procedure']['id']
        self.timeseries_procedure_label = timeseries_properties['procedure']['label']

        self.timeseries_phenomenon_id = timeseries_properties['phenomenon']['id']
        self.timeseries_phenomenon_label = timeseries_properties['phenomenon']['label']

        self.timeseries_category_id = timeseries_properties['category']['id']
        self.timeseries_category_label = timeseries_properties['category']['label']

        geometry = station['geometry']
        self.coordinates_x = geometry['coordinates'][0]
        self.coordinates_y = geometry['coordinates'][1]
        self.coordinates_type = geometry['type']


def smash(stations: list) -> list:
    smashed_stations = []
    for station in stations:
        smashed_stations = __smash_station(station)
        smashed_stations.extend(smashed_stations)
    return smashed_stations


def __smash_station(station: dict) -> list:
    properties = station['properties']
    timeseries = properties['timeseries']
    smashed_stations = []
    for key in timeseries.keys():
        value = timeseries[key]
        smashed_stations.append(SmashedStation(station, key, value))
    return smashed_stations

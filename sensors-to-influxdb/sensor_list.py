from dataclasses import dataclass
import logging
from geopy.geocoders import Nominatim

class SensorLocation:
    id: int
    country: str
    longitude: float
    latitude: float
    address: str
    city: str
    postcode: str
    
    def __init__(self, location_json):
        self.id = location_json.get('id', '')
        self.country = location_json.get('country', '')
        self.longitude = location_json.get('longitude', 0)
        self.latitude = location_json.get('latitude', 0)
        self._get_geolocation()     

    def _get_geolocation(self):
            geolocator = Nominatim(user_agent="sensor_api_collector")
            geo_location = geolocator.reverse((self.latitude, self.longitude), exactly_one=True)
            self.address = geo_location.raw['address']
            self.city = self.address.get('town', self.address.get('village', ''))
            self.postcode = self.address.get('postcode', '')

class Sensor:
    airrohr_pm_id: int
    airrohr_hum_id: int
    description: str
    location: SensorLocation

    def __init__(self, airrohr_pm_id: int, airrohr_hum_id: int, description: str):
        self.airrohr_pm_id = airrohr_pm_id
        self.airrohr_hum_id = airrohr_hum_id
        self.description = description
    
    def set_location(self, location_json):
        if not hasattr(self, 'location'):
            self.location = SensorLocation(location_json)

class SensorList:
    sensors = []

    def __init__(self, sensor_list_file : str):
        logging.info("Reading sensors from file %s", sensor_list_file)
        with open(sensor_list_file, "r") as fp:
            for line in fp.readlines():
                line_data = line.split(";")
                self.sensors.append(Sensor(line_data[0], line_data[1], line_data[2]))

    def __iter__(self):
        return iter(self.sensors)
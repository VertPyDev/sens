from sensor_list import Sensor, SensorLocation
import logging
import requests
import decimal

class SensorCommunityData():
    id: int
    timestamp: str
    sensor: Sensor
    values: dict

    def __init__(self, sensor: Sensor):
        self.sensor = sensor
        self.values = {}


class SensorCommunityDataReader():

    def __init__(self, api_url: str):
        self.api_url = api_url

    def get_data(self, sensor: Sensor):
        pm_sensor_data_list = self._read_data(sensor, sensor.airrohr_pm_id)
        hum_sensor_data_list = self._read_data(sensor, sensor.airrohr_hum_id)
        sensor_data_list = pm_sensor_data_list + hum_sensor_data_list
        if sensor_data_list:
            return sensor_data_list
        else:
            return []


    def _read_data(self, sensor: Sensor, sensor_id: int):
        logging.info("Getting data from sensor %s", sensor_id)

        request_data = requests.get(f"{self.api_url}/{sensor_id}/")

        json_request_data = request_data.json()
        logging.debug("Request Data : %s", json_request_data)

        sensor_data_list = []
        for json_sensor_data in json_request_data:
            sensor_data = SensorCommunityData(sensor)
            sensor_data.sensor.location = SensorLocation(json_sensor_data.get('location', {}))
            sensor_data.timestamp = json_sensor_data.get('timestamp')
            sensor_data.id = json_sensor_data.get('id')
            self._get_data_values(sensor_data, json_sensor_data)
            sensor_data_list.append(sensor_data)
            
        return sensor_data_list

    def _get_data_values(self, sensor_data: SensorCommunityData, json_request_data):
        for value in json_request_data.get('sensordatavalues'):
            try:
                sensor_data.values[value['value_type']] = decimal.Decimal(value['value'])
            except decimal.InvalidOperation:
                continue
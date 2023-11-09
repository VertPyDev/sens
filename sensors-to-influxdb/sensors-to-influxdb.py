import decimal
import click
import time
import requests
import influxdb_client
import logging
from config import Config
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.exceptions import InfluxDBError
from geopy.geocoders import Nominatim


SENSOR_GEO_CACHE={}

class sensors_to_influxdb:

    def __init__(self, allsensors, conf, list, sensor_id=0, debug=False):
        if debug:
            level=logging.DEBUG
        else:
            level=logging.INFO
        logging.basicConfig(format='%(asctime)s - %(message)s', level=level)
        self.__read_conf(conf)
        self.__read_sensors_list(allsensors, list, sensor_id)
        
    def __read_conf(self, conf):
        logging.info("Reading configuration file %s", conf)
        self.config=Config(conf)
        logging.debug("Configuration: %s", self.config.to_string())

    def __read_sensors_list(self, allsensors, list_file, sensor_id):
        self.sensors=[]
        if allsensors:
            logging.info("Reading sensors from file %s", list_file)
            with open(list_file, "r") as fp:
                for line in fp.readlines():
                    self.sensors.append(line.strip())
        else:
            self.sensors.append(sensor_id)
        logging.info("Sensors list: %s",  self.sensors)

    def get_data(self):
        while True:
            for sensor in self.sensors:
                self._get_data(sensor)
            time.sleep(300)


    def _get_geolocation(self, sensor_id, location):
        if sensor_id in SENSOR_GEO_CACHE:
            return SENSOR_GEO_CACHE[sensor_id]
        else:
            country = location.get('country', '')
            longitude = location.get('longitude', 0)
            latitude = location.get('latitude', 0)
            geolocator = Nominatim(user_agent="sensor_api_collector")
            geo_location = geolocator.reverse((latitude, longitude), exactly_one=True)
            address = geo_location.raw['address']
            city = address.get('town', address.get('village', ''))
            postcode = address.get('postcode', '')
            return {
                'longitude': longitude,
                'latitude': latitude,
                'country': country,
                'city': city,
                'postcode': postcode
            }



    def _get_data(self, sensor_id):
        logging.info("Getting data from sensor %s", sensor_id)
        sensor_id = int(sensor_id)

        r = requests.get(f"{self.config.api_url}/{sensor_id}/")

        data = r.json()
        logging.debug("Data : %s", data)

        for sensordata in data:
            values = {}
            location = sensordata.get('location', {})
            geo_location = {}

            if location:
                geo_location =self._get_geolocation(sensor_id, location)
            
            for value in sensordata.get('sensordatavalues'):
                try:
                    values[value['value_type']] = decimal.Decimal(value['value'])
                except decimal.InvalidOperation:
                    values = {}
                    break
            if values:
                element = {
                    'id': sensordata.get('id'),
                    'sensor_id': sensor_id,
                    'timestamp': sensordata.get('timestamp'),
                    'location': geo_location,
                    'values': values
                }
                self.add_to_influxdb(element)

    def add_to_influxdb(self, element):
        logging.debug("Writing to infuxdb : %s", element)
        client = influxdb_client.InfluxDBClient(
            url=self.config.influxdb_url,
            token=self.config.influxdb_token,
            org=self.config.influxdb_org
        )
        write_api = client.write_api(write_options=SYNCHRONOUS)
        p = influxdb_client.Point("capteurs")\
            .tag("sensor_id", element['sensor_id'])\
            .tag("country", element['location']['country'])\
            .tag("city", element['location']['city'])\
            .tag("cp", element['location']['postcode'])\
            .time(element['timestamp'])\
            .field("longitude", element['location']['longitude'])\
            .field("latitude", element['location']['latitude'])
        for key, value in element['values'].items():
            p.field(key, value)
        try:
            write_api.write(bucket=self.config.influxdb_bucket, org=self.config.influxdb_org, record=p)
        except InfluxDBError as e:
            logging.exception(e)

@click.command()
@click.option('--allsensors/--not-allsensors', default=False)
@click.option('--conf', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option('--list', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option('--sensor_id')
@click.option('--debug', default=False, type=click.BOOL)
def command(allsensors, conf, list, sensor_id=0, debug=False):
    sensors=sensors_to_influxdb(allsensors, conf, list, sensor_id, debug)
    sensors.get_data()

if __name__ == '__main__':
    command()


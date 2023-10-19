import decimal
import click
import time
import requests
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from geopy.geocoders import Nominatim
from config import *

SENSOR_GEO_CACHE={}

@click.command()
@click.option('--sensor_id')
@click.option('--allsensors/--not-allsensors', default=False)
def get_data(allsensors, sensor_id=0):
    while True:
        if allsensors:
            with open("sensor_list", "r") as fp:
                for line in fp.readlines():
                    _get_data(line.strip())
        else:
            _get_data(sensor_id)
        time.sleep(300)


def _get_geolocation(sensor_id, location):
    if sensor_id in SENSOR_GEO_CACHE:
        return SENSOR_GEO_CACHE[sensor_id]
    else:
        country = location.get('country', '')
        longitude = location.get('longitude', 0)
        latitude = location.get('latitude', 0)
        geolocator = Nominatim(user_agent="sensor_api_collector")
        geo_location = geolocator.reverse((latitude, longitude), exactly_one=True)
        address = geo_location.raw['address']
        city = address.get('village', '')
        postcode = address.get('postcode', '')
        return {
            'longitude': longitude,
            'latitude': latitude,
            'country': country,
            'city': city,
            'postcode': postcode
        }



def _get_data(sensor_id):
    sensor_id = int(sensor_id)

    r = requests.get(f"{API_URL}/{sensor_id}/")

    data = r.json()

    for sensordata in data:
        values = {}
        location = sensordata.get('location', {})
        geo_location = {}

        if location:
            geo_location =_get_geolocation(sensor_id, location)
          
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
            add_to_influxdb(element)

def add_to_influxdb(element):
    client = influxdb_client.InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG
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
    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=p)

if __name__ == '__main__':
    get_data()
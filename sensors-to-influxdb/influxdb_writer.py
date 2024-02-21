
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.exceptions import InfluxDBError
import logging
from data_reader import SensorCommunityData

class InfluxDBSensorDataWriter:
    def __init__(self, influxdb_url, influxdb_token, influxdb_org):
        self.client = influxdb_client.InfluxDBClient(
            url = influxdb_url,
            token = influxdb_token,
            org = influxdb_org
        )
        self.influxdb_org = influxdb_org

    def add_to_influxdb(self, sensor_data: SensorCommunityData, bucket):
        logging.debug("Writing to infuxdb : %s - %s", vars(sensor_data), vars(sensor_data.sensor))
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        p = influxdb_client.Point("capteurs")\
            .tag("sensor_id", sensor_data.sensor.airrohr_pm_id)\
            .tag("country", sensor_data.sensor.location.country)\
            .tag("city", sensor_data.sensor.location.city)\
            .tag("cp", sensor_data.sensor.location.postcode)\
            .time(sensor_data.timestamp)\
            .field("latitude", sensor_data.sensor.location.latitude)\
            .field("longitude", sensor_data.sensor.location.longitude)
        for key, value in sensor_data.values.items():
            p.field(key, value)
        try:
            write_api.write(bucket=bucket, org=self.influxdb_org, record=p)
        except InfluxDBError as e:
            logging.exception(e)
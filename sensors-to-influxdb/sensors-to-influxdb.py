
import click
import time
import logging
from config import Config
from sensor_list import *
from data_reader import *
from influxdb_writer import *

class sensors_to_influxdb:

    def __init__(self, conf: str, list: str):
        self.config = Config(conf)
        if self.config.debug:
            level = logging.DEBUG
        else:
            level = logging.INFO
        logging.basicConfig(format='%(asctime)s - %(message)s', level=level)
        logging.debug("Configuration: %s", self.config.to_string())
        self.sensors = SensorList(list)
        self.sensor_community_data_reader = SensorCommunityDataReader(self.config.api_url)
        self.influxdb_sensor_data_writer = InfluxDBSensorDataWriter(self.config.influxdb_url, self.config.influxdb_token, self.config.influxdb_org)
        
    def get_data(self):
        while True:
            try:
                for sensor in self.sensors:
                    sensor_data_list = self.sensor_community_data_reader.get_data(sensor)
                    for data in sensor_data_list:
                        self.influxdb_sensor_data_writer.add_to_influxdb(data, self.config.influxdb_bucket)
                time.sleep(self.config.loop_time)
            except:
                logging.exception()

@click.command()
@click.option('--conf', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option('--list', type=click.Path(exists=True, file_okay=True, dir_okay=False))
def command(conf, list):
    sensors=sensors_to_influxdb(conf, list)
    sensors.get_data()

if __name__ == '__main__':
    command()


import configparser
import logging

class Config:
    def __init__(self, conf_file : str):
        logging.info("Reading configuration file %s", conf_file)
        config = configparser.ConfigParser()
        config.read(conf_file)
        self.influxdb_url=config['influxdb']['URL']
        self.influxdb_token=config['influxdb']['TOKEN']
        self.influxdb_org=config['influxdb']['ORG']
        self.influxdb_bucket=config['influxdb']['BUCKET']
        self.api_url=config['sensors-api']['URL']
        self.loop_time=int(config['global']['LOOP_TIME'])
        logging.debug("Configuration: %s", self.to_string())

    def to_string(self):
        return f"INFLUXDB_URL: {self.influxdb_url}, INFLUXDB_ORG: {self.influxdb_org}, INFLUXDB_BUCKET: {self.influxdb_bucket}, API_URL: {self.api_url}"
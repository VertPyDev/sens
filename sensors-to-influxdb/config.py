import configparser

class Config:
    def __init__(self, conf_file : str):
        config = configparser.ConfigParser()
        config.read(conf_file)
        self.influxdb_url=config['influxdb']['URL']
        self.influxdb_token=config['influxdb']['TOKEN']
        self.influxdb_org=config['influxdb']['ORG']
        self.influxdb_bucket=config['influxdb']['BUCKET']
        self.api_url=config['sensors-api']['URL']
        self.loop_time=int(config['global']['LOOP_TIME'])
        self.debug=config.getboolean('global', 'DEBUG')

    def to_string(self):
        return f"INFLUXDB_URL: {self.influxdb_url}, INFLUXDB_ORG: {self.influxdb_org}, INFLUXDB_BUCKET: {self.influxdb_bucket}, API_URL: {self.api_url}"
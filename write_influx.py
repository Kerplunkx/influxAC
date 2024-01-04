from influxdb_client import InfluxDBClient, Point
from datetime import datetime

class InfluxLogic:
    def __init__(self, url, token, org, bucket):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.bucket = bucket
        self.org = org
        self.token = token
        self.write_api = self.client.write_api()

    def write_data(self, total_sum):
        point = (
            Point("potencia")
            .tag("potencia", "potencia")
            .field("potencia", total_sum)
        )
        self.write_api.write(bucket=self.bucket, org=self.org, record=point)

    def close(self):
        self.client.close()

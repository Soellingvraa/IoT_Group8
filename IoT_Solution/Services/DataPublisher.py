#Data_Modification.py
#This module should import data from rabbit mq and
# modify it before sending it back to the rabbitMQ but
# it should also be available locally

import math
import json
import time
from collections import deque
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

#Config
MQTT_Broker = 'localhost' #If the docker-compose file is set up correctly, this should be the name of the rabbitmq service defined in the docker-compose file
MQTT_Topic = 'nimbus/sensor_data' #This is the topic that the data will be published to and subscribed from, it should be the same as the one used in the NimbusServer.py file
InfluxURL = 'http://localhost:8086' #This is the URL of the InfluxDB instance, if the docker-compose file is set up correctly, this should be the name of the influxdb service defined in the docker-compose file followed by :8086
InfluxToken = 'IQAMaZqXSE6fZBPZTqfGKpiPGo1jJZJ6yZVov0i0YYdKa5oGjYvKEOpyCVHcZ9NPWQVRBmouATpRvWz5CR_bDQ=='   #This might not work  #This is the token for the InfluxDB instance, it should be the same as the one defined in the docker-compose file
InfluxOrg = 'Nimbus'           #This is the organization for the InfluxDB instance, it should be the same as the one defined in the docker-compose file
InfluxBucket = 'Nimbus' 


class DataModification:
    def __init__(self, window = 10): #window is the number of data points to consider for the moving average, 
        #it can be changed when creating an instance of the DataModification class
        # Moving average setup    
                # InfluxDB client setup
        self.influx_client = InfluxDBClient(url=InfluxURL, token=InfluxToken, org=InfluxOrg)
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)

    def data_publisher(self, client, userdata, msg):
        """ the engine that processes incoming data from rabbitMQ, it is called every time a new message is received on the subscribed topic
        """
        try: 
            raw_data = json.loads(msg.payload.decode('utf-8'))
            t = raw_data['temperature']
            h = raw_data['humidity']
            p = raw_data['pressure']

            avg_temp = self.calculate_moving_average(t, self.temp_window)
            avg_humidity = self.calculate_moving_average(h, self.humidity_window)
            avg_pressure = self.calculate_moving_average(p, self.pressure_window)


            self.historical_extremes(t, h, p)


            point = Point("IoT_Data_raw")\
                .tag("Nimbus", "Raw Values")\
                .field("Temperature", t)\
                .field("Humidity", h)\
                .field("Pressure", p)\
                .time(time.time_ns(), WritePrecision.NS)
            self.write_api.write(bucket=InfluxBucket, org=InfluxOrg, record=point)
            print(f"logged raw data to InfluxDB: {point.to_line_protocol()}")

        except Exception as e:
            print(f"Error: {e}")
            


    
#main execution
if __name__ == "__main__":
    service = DataModification(window=10)
    MQTT_c =  mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    MQTT_c.on_message = service.data_publisher
    MQTT_c.username_pw_set("Nimbus", "Nimbus")

    MQTT_c.connect(MQTT_Broker, 1883, 60)
    MQTT_c.subscribe(MQTT_Topic)

    print("Publishing Raw data to InfluxDB")
    MQTT_c.loop_forever()    
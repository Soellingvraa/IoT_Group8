#sensors.py
#sense hat interaction logic
#This module should be responsible for interacting with the sense hat and pulling data from it, 
# it is a service that is used by the raspberry pi 2, that only pulls data from the sensehat while another script is responsible for uploading the data to rabbitMQ

from sense_hat import SenseHat
sense = SenseHat()

class WeatherSensor:
    def __init__(self):
        self.sense = SenseHat()

    def get_readings(self):
        return {
            "temperature": float(round(self.sense.get_temperature(), 1)),
            "humidity":    float(round(self.sense.get_humidity(), 1)),
            "pressure":    float(round(self.sense.get_pressure(), 1))
        }



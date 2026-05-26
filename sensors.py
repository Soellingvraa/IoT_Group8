#sensors.py
#sense hat interaction logic


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



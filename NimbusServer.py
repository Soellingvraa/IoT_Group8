#NimbusServer.py
#running on the raspberry pi, must upload the data pulled from the sense hat
#to rabbitMQ with a changable frequency
#the data should also be stored locally in a database for later use and analysis
import time
import json
import threading
import paho.mqtt.client as mqtt
from flask import Flask, render_template_string, jsonify

from sensors import WeatherSensor  # pulling the WeatherSensor class form sensors.py
app = Flask(__name__)
data = WeatherSensor()  # Initial data fetch

# MQTT configuration
MQTT_BROKER = '192.168.32.8'  # Change this to your RabbitMQ broker address (local IP address of RabbitMQ server)
MQTT_TOPIC = 'nimbus/sensor_data'

#local data storage, this is just a placeholder, you can replace it with a proper database implementation 
#we should probably move this to a seperate module, but for now it is here for simplicity
current_data = {"temperature": None, "humidity": None, "pressure": None}

def mqtt_publish_data():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set("Nimbus", "Nimbus")  # Set your MQTT username and password
    try:
        client.connect(192.168.32.8, 1883, 60)
        print(f"Connected to MQTT broker at {MQTT_BROKER}")
        while True:
            Weather_data = data.get_readings()
            client.publish(MQTT_TOPIC, json.dumps(Weather_data))
            time.sleep(10)  # Publish data every 10 seconds
    except Exception as e:
        print(f"Failed to connect to MQTT broker: {e}")
        return

threading.Thread(target=mqtt_publish_data, daemon=True).start()

def on_connect(client, userdata, flags, rc, properties=None): #only for debugging to see if it actually connects to the broker, not used for anything else 
    # code 1 = incorrect protocol version, code 2 = invalid client identifier, code 3 = server unavailable, code 4 = bad username or password, code 5 = not authorized
    if rc == 0:
        print(" SUCCESS: Connected to PC Broker")
    else:
        print(f" FAILED: Connection refused, error code: {rc}")

#HTML dashboard template
dashboard_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title> Welcome to the Nimbus Weater Station Dashboard! </title>
    <style>
        body {
            font-family: Arial, sans-serif; background: #1a1a1a; color: white; text-align: center; padding: 50px;}
            .card {background: #2d2d2d; border-radius: 15px; padding:20px; margin: 20px auto; display: inline-block; margin: 10px; min-width: 200px; box-shadow: 0 4px 8px rgba(0,0,0,0.5);}
            h1 {color: #00ff99;}
            .value {font-size: 2.5em; font-weight: bold; color: #00ccff;}
            .unit {font-size 0.5em; color: #aaa;}
    </style>
</head>

<body>
    <h1>Nimbus Weather Station</h1>
    <div class="card"><div>Temperature</div><div id="temp" class="value">{{ temp }}</div><span class="unit">°C</span></div>
    <div class="card"><div>Humidity</div><div id="humidity" class="value">{{ humidity }}</div><span class="unit">%</span></div>
    <div class="card"><div>Pressure</div><div id="pressure" class="value">{{ pressure }}</div><span class="unit">hPa</span></div>
    
    <script>
    let intervalSeconds = 10; // You can change this to any number

    function updateDashboard() {
        fetch('/api/data')
            .then(response => response.json())
            .then(data => {
                document.getElementById('temp').innerText = data.temperature;
                document.getElementById('humidity').innerText = data.humidity;
                document.getElementById('pressure').innerText = data.pressure;
            });
    }

    // Run the update every X seconds
    setInterval(updateDashboard, intervalSeconds * 1000);
</script>
</body>
</html>
"""
@app.route('/')
def index():
    # Initial load of the page
    weather_data = data.get_readings()
    return render_template_string(dashboard_template,
                                   temp=weather_data["temperature"],
                                    humidity=weather_data["humidity"],
                                    pressure=weather_data["pressure"])

@app.route('/api/data')
def getdata():
    weather_data = data.get_readings()
    return jsonify(weather_data)

if __name__ == '__main__':
    # Use port 5000 so we don't always need 'sudo'
    app.run(host='0.0.0.0', port=5000)

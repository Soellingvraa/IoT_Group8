#NimbusServer.py
#running on the raspberry pi, must upload the data pulled from the sense hat
#to rabbitMQ with a changable frequency
#the data should also be stored locally in a database for later use and analysis
import time
import json
import threading
import paho.mqtt.client as mqtt
from flask import Flask, render_template_string, jsonify, request
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from sensors import WeatherSensor  # pulling the WeatherSensor class form sensors.py

app = Flask(__name__)
data = WeatherSensor()  # Initial data fetch
config= {
    "publish_interval": 10,  # publishing delay set via api, default is 10 seconds
    "temp_offset": 0.0      #initial temperature offset, can be changed via the API
}
alerts = {}            
alert_id_counter = 1    

# MQTT configuration
MQTT_BROKER = '192.168.32.8'  # Change this to your RabbitMQ broker address (local IP address of RabbitMQ server)
MQTT_TOPIC = 'nimbus/sensor_data'

#influxDB config
MQTT_Broker = '192.168.32.8' #If the docker-compose file is set up correctly, this should be the name of the rabbitmq service defined in the docker-compose file
MQTT_Topic = 'nimbus/sensor_data' #This is the topic that the data will be published to and subscribed from, it should be the same as the one used in the NimbusServer.py file
InfluxURL = 'http://192.168.32.8:8086' #This is the URL of the InfluxDB instance, if the docker-compose file is set up correctly, this should be the name of the influxdb service defined in the docker-compose file followed by :8086
InfluxToken = 'xYitY0VJ5e5hyCt6uR-fhA2NiCNlxbwl_QAj8_Xj-VIiy5VsRWauLodSQMtLLYr1ANnxXrwNSH_Tz0jBQMpqjQ=='   #This might not work  #This is the token for the InfluxDB instance, it should be the same as the one defined in the docker-compose file
InfluxOrg = 'Nimbus'           #This is the organization for the InfluxDB instance, it should be the same as the one defined in the docker-compose file
InfluxBucket = 'Nimbus' 

#local data storage, this is just a placeholder, you can replace it with a proper database implementation 
#we should probably move this to a seperate module, but for now it is here for simplicity
current_data = {"temperature": None, "humidity": None, "pressure": None}

def on_config_message(client, userdata, msg):
    global config
    try:
        new_config = json.loads(msg.payload.decode('utf-8'))
        config.update(new_config)
        print(f"[QoS 2] Config applied: {new_config}")
    except Exception as e:
        print(f"Config error: {e}")
def start_config_listener():
    config_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    config_client.username_pw_set("Nimbus", "Nimbus")
    config_client.on_message = on_config_message

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            # Subscribe at QoS 2 — must be inside on_connect
            # so it resubscribes automatically on reconnect
            client.subscribe('nimbus/config/rpi2', qos=2)
            print("[QoS 2] Subscribed to config topic ✓")

    def on_subscribe(client, userdata, mid, reason_codes, properties=None):
        for rc in reason_codes:
            granted = rc.value if hasattr(rc, 'value') else rc
            if granted == 2:
                print("[QoS 2] Config subscription confirmed ✓")
            else:
                print(f"WARNING: Broker only granted QoS {granted} for config topic!")

    config_client.on_connect = on_connect
    config_client.on_subscribe = on_subscribe
    config_client.connect(MQTT_BROKER, 1883, 60)
    config_client.loop_forever()

# Start the config listener as a separate daemon thread
threading.Thread(target=start_config_listener, daemon=True).start()
def check_alerts(readings):
    for alert in alerts.values():
        sensor_value = readings[alert['sensor']]
        threshold    = alert['threshold']

        if alert['condition'] == 'above' and sensor_value > threshold:
            alert['triggered'] = True
            print(f"ALERT: {alert['sensor']} is {sensor_value}, above {threshold}")
        elif alert['condition'] == 'below' and sensor_value < threshold:
            alert['triggered'] = True
            print(f"ALERT: {alert['sensor']} is {sensor_value}, below {threshold}")
        else:
            alert['triggered'] = False

def mqtt_publish_data():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set("Nimbus", "Nimbus")  # Set your MQTT username and password
    client.connect(MQTT_BROKER, 1883, 60)
    try:
        print(f"Connected to MQTT broker at {MQTT_BROKER}")
        while True:
            Weather_data = data.get_readings()
            Weather_data['temperature'] = round(
            Weather_data['temperature'] + config['temp_offset'], 1
                )
            t = Weather_data['temperature']
            h = Weather_data['humidity']
            p = Weather_data['pressure']
            time.sleep(10)  # Publish data every 10 seconds
            try: 
                point = Point("IoT Sensor Data raw")\
                    .tag("Nimbus", "Raw Values")\
                    .field("Temperature", t)\
                    .field("Humidity", h)\
                    .field("Pressure", p)\
                    .time(time.time_ns(), WritePrecision.NS)
                influx_client = InfluxDBClient(url=InfluxURL, token=InfluxToken, org=InfluxOrg)
                write_api = influx_client.write_api(write_options=SYNCHRONOUS)
                write_api.write(bucket=InfluxBucket, org=InfluxOrg, record=point)
                print(f"logged raw data to InfluxDB: {point.to_line_protocol()}")
                check_alerts(Weather_data)
                client.publish(MQTT_TOPIC, json.dumps(Weather_data), qos=0)
            except Exception as e:
                print(f"InfluxDB Error: {e}")
            time.sleep(config['publish_interval']) #publish delay variable
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
            .alert-row { display: flex; justify-content: space-between; align-items: center;
            background: #2d2d2d; border-radius: 10px; padding: 12px 16px; margin: 8px 0; }
            .alert-row.triggered { border: 1px solid #ff4444; background: #3a1a1a; }
            .badge { padding: 3px 10px; border-radius: 99px; font-size: 12px; font-weight: bold; }
            .badge-ok { background: #1a3a1a; color: #00ff99; }
            .badge-triggered { background: #3a1a1a; color: #ff4444; }
            .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 8px; }
            .dot-ok { background: #00ff99; }
            .dot-alert { background: #ff4444; animation: pulse 1.2s infinite; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
    </style>
</head>
<h2 style="color:#00ff99; margin-top: 40px;">Alert Rules</h2>
<div id="alert-list" style="max-width: 600px; margin: 0 auto;"></div>
<body>
    <h1>Nimbus Weather Station</h1>
    <div class="card"><div>Temperature</div><div id="temp" class="value">{{ temp }}</div><span class="unit">°C</span></div>
    <div class="card"><div>Humidity</div><div id="humidity" class="value">{{ humidity }}</div><span class="unit">%</span></div>
    <div class="card"><div>Pressure</div><div id="pressure" class="value">{{ pressure }}</div><span class="unit">hPa</span></div>
    
    <script>
    function updateDashboard() {
    fetch('/api/data')
        .then(r => r.json())
        .then(data => {
            document.getElementById('temp').innerText     = data.temperature;
            document.getElementById('humidity').innerText = data.humidity;
            document.getElementById('pressure').innerText = data.pressure;

            const list = document.getElementById('alert-list');
            list.innerHTML = '';

            if (data.alerts.length === 0) {
                list.innerHTML = '<p style="color:#aaa;font-size:14px;">No alert rules configured.</p>';
                return;
            }

            data.alerts.forEach(alert => {
                const triggered = alert.triggered;
                const row = document.createElement('div');
                row.className = 'alert-row' + (triggered ? ' triggered' : '');
                row.innerHTML = `
                    <div>
                        <span class="dot ${triggered ? 'dot-alert' : 'dot-ok'}"></span>
                        <strong>${alert.sensor} ${alert.condition} ${alert.threshold}</strong>
                    </div>
                    <span class="badge ${triggered ? 'badge-triggered' : 'badge-ok'}">
                        ${triggered ? 'Triggered' : 'OK'}
                    </span>`;
                list.appendChild(row);
            });
        });
}

setInterval(updateDashboard, 10000);
updateDashboard();
</script>
</body>
</html>
"""


#Flask routes
#REST APIs
#rune wanted us to expand a bit on this and maybe implement so that we can change data in the weatherstation via the api 
#maybe implement one of each: get, post, put, delete

@app.route('/')
def index():
    # Initial load of the page
    weather_data = data.get_readings()
    return render_template_string(dashboard_template,
                                   temp=weather_data["temperature"],
                                    humidity=weather_data["humidity"],
                                    pressure=weather_data["pressure"])

@app.route('/api/data', methods=['GET'])
def get_current():
    return jsonify(data.get_readings())
# @app.route('/api/data')
# def getdata():
#     weather_data = data.get_readings()
#     check_alerts(weather_data)  # run the checker on every poll
#     return jsonify({
#         "temperature": weather_data["temperature"],
#         "humidity":    weather_data["humidity"],
#         "pressure":    weather_data["pressure"],
#         "alerts":      list(alerts.values())   # include alerts in the response
#     })
@app.route('/api/data/history', methods=['GET'])
def get_history():
    # Query InfluxDB for the last N readings
    # e.g. ?limit=100&sensor=temperature
    limit = request.args.get('limit', 50)
    return jsonify({"message": f"Last {limit} readings"})

@app.route('/api/alerts', methods=['POST'])
def create_alert():
    global alert_id_counter
    body = request.get_json()

    if not all(k in body for k in ['sensor', 'threshold', 'condition']):
        return jsonify({"error": "Missing fields"}), 400
    if body['sensor'] not in ['temperature', 'humidity', 'pressure']:
        return jsonify({"error": "Invalid sensor"}), 400
    if body['condition'] not in ['above', 'below']:
        return jsonify({"error": "condition must be above or below"}), 400

    alert = {
        "id":        alert_id_counter,
        "sensor":    body['sensor'],
        "threshold": body['threshold'],
        "condition": body['condition'],
        "triggered": False
    }
    alerts[alert_id_counter] = alert
    alert_id_counter += 1
    return jsonify(alert), 201

# Persistent client for publishing config commands
config_publisher = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
config_publisher.username_pw_set("Nimbus", "Nimbus")
config_publisher.connect(MQTT_BROKER, 1883, 60)
config_publisher.loop_start()  # non-blocking loop for the publisher

@app.route('/api/config', methods=['PUT'])
def update_config():
    body = request.get_json()

    # Validate fields
    allowed = {'publish_interval', 'temp_offset'}
    if not any(k in body for k in allowed):
        return jsonify({"error": "No valid config fields provided"}), 400

    # Publish at QoS 2 — exactly once delivery
    result = config_publisher.publish(
        'nimbus/config/rpi2',
        json.dumps(body),
        qos=2           # <-- QoS 2 here
    )

    # Wait for the full 4-step QoS 2 handshake to complete
    result.wait_for_publish()

    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        return jsonify({
            "message": "Config command delivered (QoS 2)",
            "mid": result.mid,
            "config": body
        }), 200
    else:
        return jsonify({"error": f"Publish failed: {result.rc}"}), 500
    
@app.route('/api/alerts/<int:alert_id>', methods=['DELETE'])
def delete_alert(alert_id):
    if alert_id not in alerts:
        return jsonify({"error": "Alert not found"}), 404
    del alerts[alert_id]
    return jsonify({"message": f"Alert {alert_id} deleted"}), 200

if __name__ == '__main__':
    # Use port 5000 so we don't always need 'sudo'
    app.run(host='0.0.0.0', port=5000)

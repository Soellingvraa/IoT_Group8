from flask import Flask, render_template_string
from sense_hat import SenseHat

app = Flask(__name__)
sense = SenseHat()


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
    <div class="card"><div>Temperature</div><div class="value">{{ temp }}<span class="unit">°C</span></div></div>
    <div class="card"><div>Humidity</div><div class="value">{{ humidity }}<span class="unit">%</span></div></div>
    <div class="card"><div>Pressure</div><div class="value">{{ pressure }}<span class="unit">hPa</span></div></div>
</body>
</html>
"""

@app.route('/')
def index():
    t = round(sense.get_temperature(), 1)
    h = round(sense.get_humidity(), 1)
    p = round(sense.get_pressure(), 1)
    return render_template_string(dashboard_template, temp=t, humidity=h, pressure=p)

if __name__ == '__main__':
    # Use port 5000 so we don't always need 'sudo'
    app.run(host='0.0.0.0', port=5000)

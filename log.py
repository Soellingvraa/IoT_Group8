#logger function
import csv
import os
from datetime import datetime

def log_weather_data(temp, humidity, pressure, filename="weatherlog.csv"):
    file_exists = os.path.isfile(filename)
    try: 
        with open (filename, mode = 'a', newline = '') as file:
            writer = csv.writer (file)

            if not file_exists:
                writer.writerow(['Time', 'Temperature', 'Humidity'])

                time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                writer.writerow([time,temp,humidity,pressure])

        return True
    except Exception as e:
        print(f"Error: logging data: {e}")
        return False
import psycopg2
import requests
import json
conn = psycopg2.connect(
    host="192.168.2.49",
    port="5432",
    database="HMS_source",
    user="postgres",
    password="Red*St0ne"
)

conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

cur = conn.cursor()

# Listen for notifications
cur.execute("LISTEN api_notification;")

input1 = None
input2 = None

try:
    while True:
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            data = json.loads(notify.payload)
            # Access the data values
            input1 = data['input1']
            input2 = data['input2']
            # Process the received data from the notification
            print("Input 1:", input1)
            print("Input 2:", input2)

except KeyboardInterrupt:
    print("Interrupted by user")

except Exception as e:
    print("An error occurred:", str(e))

finally:
    cur.close()
    conn.close()

# Print the last received values outside the loop
origin=input1
destination=input2
print("Last received values - Input 1:", origin)
print("Last received values - Input 2:", destination)
import googlemaps
from datetime import datetime

# Google Maps API key
api_key = 'AIzaSyDstQXK7snU0tN_Y6wJ2NF3eRUIs3ZFxIg'
gmaps = googlemaps.Client(key=api_key)

# Address definition (Address or EIR Code)
# origin = '90 Whitestown Park, Blanchardstown'
# destination = 'D15 NR7N'

# Depart at filter (Example: June 13, 2023, 9:00 AM)
departure_time = datetime.now()

# Fetch Car travel Distance, Duration & Route
car_directions = gmaps.directions(origin, destination, mode='driving', departure_time=departure_time)
car_distance = car_directions[0]['legs'][0]['distance']['text']
car_duration = car_directions[0]['legs'][0]['duration']['text']
car_route = car_directions[0]['summary']

# Fetch Bus travel Distance, Duration
bus_directions = gmaps.directions(origin, destination, mode='transit', departure_time=departure_time)
bus_distance = bus_directions[0]['legs'][0]['distance']['text']
bus_duration = bus_directions[0]['legs'][0]['duration']['text']

# Fetch Walking Distance, Duration & Route
walking_directions = gmaps.directions(origin, destination, mode='walking')
walking_distance = walking_directions[0]['legs'][0]['distance']['text']
walking_duration = walking_directions[0]['legs'][0]['duration']['text']
walking_route = walking_directions[0]['summary']

# Output
print("Depart Time:", departure_time)
print("Car Route:", "via "+car_route)
print("Car travel distance:", car_distance)
print("Car travel duration:", car_duration)
print("Bus travel distance:", bus_distance)
print("Bus travel duration:", bus_duration)
print("Walking Route:", "via "+walking_route)
print("Walking distance:", walking_distance)
print("Walking duration:", walking_duration)
from flask import Flask, jsonify, render_template
import psycopg2
import json
import googlemaps
from datetime import datetime

app = Flask(__name__)

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


def perform_calculations(input1, input2):
    try:
        # Google Maps API key
        api_key = 'AIzaSyDstQXK7snU0tN_Y6wJ2NF3eRUIs3ZFxIg'
        gmaps = googlemaps.Client(key=api_key)

        # Depart at filter (Example: June 13, 2023, 9:00 AM)
        departure_time = datetime.now()   # if datetime is in the past it will give an error

        # Fetch Car travel Distance, Duration & Route
        car_directions = gmaps.directions(input1, input2, mode='driving', departure_time=departure_time)
        car_distance = car_directions[0]['legs'][0]['distance']['text']
        car_duration = car_directions[0]['legs'][0]['duration']['text']
        car_route = car_directions[0]['summary']

        # Fetch Bus travel Distance, Duration
        bus_directions = gmaps.directions(input1, input2, mode='transit', departure_time=departure_time)
        if bus_directions:
            bus_distance = bus_directions[0]['legs'][0]['distance']['text']
            bus_duration = bus_directions[0]['legs'][0]['duration']['text']
        else:
            bus_distance = 'N/A'
            bus_duration = 'N/A'

        # Fetch Walking Distance, Duration & Route
        walking_directions = gmaps.directions(input1, input2, mode='walking')
        walking_distance = walking_directions[0]['legs'][0]['distance']['text']
        walking_duration = walking_directions[0]['legs'][0]['duration']['text']
        walking_route = walking_directions[0]['summary']

        # Output
        print("Depart Time:", departure_time)
        print("Car Route:", "via " + car_route)
        print("Car travel distance:", car_distance)
        print("Car travel duration:", car_duration)
        print("Bus travel distance:", bus_distance)
        print("Bus travel duration:", bus_duration)
        print("Walking Route:", "via " + walking_route)
        print("Walking distance:", walking_distance)
        print("Walking duration:", walking_duration)

        return {
            'input1': input1,
            'input2': input2,
            'Depart_Time': departure_time,
            'Car_Route': car_route,
            'Car_Distance': car_distance,
            'Car_Duration': car_duration,
            'Bus_Distance': bus_distance,
            'Bus_Duration': bus_duration,
            'Walk_Route': walking_route,
            'Walk_Distance': walking_distance,
            'Walk_Duration': walking_duration
        }
    except Exception as e:
        print("An error occurred during calculations:", str(e))
        return None


@app.route('/api', methods=['GET'])
def get_last_received_values():
    global conn

    # Reload the page
    return render_template('result.html')


@app.route('/listen')
def listen():
    global conn

    conn.poll()
    while conn.notifies:
        notify = conn.notifies.pop(0)
        data = json.loads(notify.payload)

        # Access the data values
        input1 = data['input1']  # input1 = Origin
        input2 = data['input2']  # input2 = Destination

        # Process the received data from the notification
        print("Input 1:", input1)
        print("Input 2:", input2)

        # Perform calculations and insert data into the database
        calculated_values = perform_calculations(input1, input2)

        if calculated_values:
            # Insert the calculated values into the database
            insert_query = """
                INSERT INTO travel_info (input1, input2, Depart_Time, Car_Route, Car_Distance, Car_Duration, 
                Bus_Distance, Bus_Duration, Walk_Route, Walk_Distance, Walk_Duration)
                VALUES (%(input1)s, %(input2)s, %(Depart_Time)s, %(Car_Route)s, %(Car_Distance)s, %(Car_Duration)s,
                %(Bus_Distance)s, %(Bus_Duration)s, %(Walk_Route)s, %(Walk_Distance)s, %(Walk_Duration)s)
            """
            try:
                cur.execute(insert_query, calculated_values)
                conn.commit()
            except Exception as e:
                print("An error occurred while inserting data into the database:", str(e))

    return "OK"


if __name__ == '__main__':
    app.run(debug=True)

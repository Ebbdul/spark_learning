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

@app.route('/api', methods=['GET'])
def get_last_received_values():
    global conn

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

        # Google Maps API key
        api_key = 'AIzaSyDstQXK7snU0tN_Y6wJ2NF3eRUIs3ZFxIg'
        gmaps = googlemaps.Client(key=api_key)

        # Address definition (Address or EIR Code)
        origin = input1
        destination = input2

        # Depart at filter (Example: June 13, 2023, 9:00 AM)
        departure_time = datetime.now()

        try:
            # Fetch Car travel Distance, Duration & Route
            car_directions = gmaps.directions(origin, destination, mode='driving', departure_time=departure_time)
            car_distance = car_directions[0]['legs'][0]['distance']['text']
            car_duration = car_directions[0]['legs'][0]['duration']['text']
            car_route = car_directions[0]['summary']
        except Exception as e:
            # Handle API call error
            print(f"Error fetching car directions: {e}")
            return jsonify({'error': 'Unable to fetch car directions.'})

        try:
            # Fetch Bus travel Distance, Duration
            bus_directions = gmaps.directions(origin, destination, mode='transit', departure_time=departure_time)
            if bus_directions:
                bus_distance = bus_directions[0]['legs'][0]['distance']['text']
                bus_duration = bus_directions[0]['legs'][0]['duration']['text']
            else:
                bus_distance = 'N/A'
                bus_duration = 'N/A'
        except Exception as e:
            # Handle API call error
            print(f"Error fetching bus directions: {e}")
            return jsonify({'error': 'Unable to fetch bus directions.'})

        try:
            # Fetch Walking Distance, Duration & Route
            walking_directions = gmaps.directions(origin, destination, mode='walking')
            walking_distance = walking_directions[0]['legs'][0]['distance']['text']
            walking_duration = walking_directions[0]['legs'][0]['duration']['text']
            walking_route = walking_directions[0]['summary']
        except Exception as e:
            # Handle API call error
            print(f"Error fetching walking directions: {e}")
            return jsonify({'error': 'Unable to fetch walking directions.'})

        # Output
        dt = "Depart Time: " + str(departure_time)
        car_rt = "Car Route: " + "via " + car_route
        car_dis = "Car travel distance: " + car_distance
        car_dur = "Car travel duration: " + car_duration
        bus_dis = "Bus travel distance: " + bus_distance
        bus_dur = "Bus travel duration: " + bus_duration
        walk_rt = "Walking Route: " + "via " + walking_route
        walk_dis = "Walking distance: " + walking_distance
        walk_dur = "Walking duration: " + walking_duration

        print(dt)
        print(car_rt)
        print(car_dis)
        print(car_dur)
        print(bus_dis)
        print(bus_dur)
        print(walk_rt)
        print(walk_dis)
        print(walk_dur)

        try:
            # Insert the calculated values into the database
            insert_query = """
                        INSERT INTO calculated_distance (origin, destination, car_route, car_distance,
                                                car_duration, bus_distance, bus_duration, walk_route, walk_distance,
                                                walk_duration, depart_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
            values = (input1, input2, car_route, car_distance, car_duration, bus_distance,
                      bus_duration, walking_route, walking_distance, walking_duration, departure_time,)

            cur.execute(insert_query, values)
            conn.commit()
        except Exception as e:
            # Handle database insertion error
            print(f"Error inserting values into the database: {e}")
            return jsonify({'error': 'Unable to insert values into the database.'})

        # Return the last received values as a JSON response
        return render_template('result.html', dt=dt, car_rt=car_rt, car_dis=car_dis, car_dur=car_dur,
                               bus_dis=bus_dis, bus_dur=bus_dur, walk_rt=walk_rt, walk_dis=walk_dis,
                               walk_dur=walk_dur)


if __name__ == '__main__':
    app.run(debug=True)

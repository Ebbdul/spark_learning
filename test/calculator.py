# Import Libraries
import googlemaps
from datetime import datetime

# Google Maps API key
api_key = 'AIzaSyDstQXK7snU0tN_Y6wJ2NF3eRUIs3ZFxIg'
gmaps = googlemaps.Client(key=api_key)

# Address definition (Address or EIR Code)
# origin = 'Khana Police Station Mosque, J4J7+226, Khanna Islamabad, Rawalpindi, Islamabad Capital Territory, Pakistan'
# destination = 'Faizabad, Rawalpindi, Islamabad, Pakistan'
origin='90 Whitestown Park, Blanchardstown'
destination='D15 NR7N'
# Depart at filter (Example: June 13, 2023, 9:00 AM)
departure_time = datetime.now()

# Fetch Car travel Distance, Duration & Route
car_directions = gmaps.directions(origin, destination, mode='driving', departure_time=departure_time)
car_distance = car_directions[0]['legs'][0]['distance']['text']
car_duration = car_directions[0]['legs'][0]['duration']['text']
car_route = car_directions[0]['summary']

# Fetch Bus travel Distance, Duration
bus_directions = gmaps.directions(origin, destination, mode='transit', departure_time=departure_time)
if bus_directions:
    bus_distance = bus_directions[0]['legs'][0]['distance']['text']
    bus_duration = bus_directions[0]['legs'][0]['duration']['text']
else:
    bus_distance = 'N/A'
    bus_duration = 'N/A'

# Fetch Walking Distance, Duration & Route
walking_directions = gmaps.directions(origin, destination, mode='walking')
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

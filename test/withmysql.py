import mysql.connector
import googlemaps
from datetime import datetime
# Connect to the MySQL database
cnx = mysql.connector.connect(
    host='localhost',
    user='root',
    password='123@abc',
    database='test'
)

# Create a cursor object to interact with the database
cursor = cnx.cursor()

# Define the MySQL stored procedure
cursor.execute('DROP PROCEDURE IF EXISTS test123;')
procedure_definition = """
    
    CREATE PROCEDURE test123(
      IN input1 VARCHAR(500),
      IN input2 VARCHAR(500)
    )
    BEGIN
      DROP TABLE IF EXISTS testingvalue;
      CREATE TABLE testingvalue(value1 VARCHAR(500), value2 VARCHAR(500));
      INSERT INTO testingvalue (value1, value2)
      VALUES (input1, input2);
    END;
"""

# Execute the procedure creation statement
cursor.execute(procedure_definition)

# Call the stored procedure
input_data = ('90 Whitestown Park, Blanchardstown', 'D15 NR7N')
cursor.callproc('test123', input_data)

# Commit the changes to the database

cursor.execute('select value1, value2 from testingvalue')
result= cursor.fetchall()
outpu1=[]
output2=[]
for i in result:
    outpu1.append(i[0])
    output2.append(i[1])




cnx.commit()
# Close the cursor and database connection
cursor.close()
cnx.close()
origin=''.join(outpu1)
destination = ''.join(output2)

# print(origin)
# print(destination)
departure_time = datetime.now()
#   calculation goes here
# Import Libraries


# Google Maps API key
api_key = 'AIzaSyDstQXK7snU0tN_Y6wJ2NF3eRUIs3ZFxIg'
gmaps = googlemaps.Client(key=api_key)

# Address definition (Address or EIR Code)
# origin = '90 Whitestown Park, Blanchardstown'
# destination = 'D15 NR7N'
#
# # Depart at filter (Example: June 13, 2023, 9:00 AM)
# departure_time = datetime(2023, 6, 14, 12, 0)

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
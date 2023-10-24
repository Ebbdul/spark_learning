from flask import Flask, jsonify
from datetime import datetime
# from calculator import  origin, destination, departure_time
app = Flask(__name__)

@app.route('/', methods=['GET'])
def get_example():
    origin = input("Enter your Origin: ")
    destination = input("Enter your Destination: ")
    departure_time=datetime(input("Enter the Time:"))

    message = f"The Origin is, {origin}! and the Destination is {destination} and the departure time taken is {departure_time}."
    data = {
        'message': message,
        'status': 'success'
    }

    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)

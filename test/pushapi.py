from flask import Flask, request

app = Flask(__name__)

@app.route('/', methods=['POST'])
def create_user():
    data = request.json  # Get the JSON data from the request body
    name = data['name']
    age = data['age']
    # Process the data and create a new user
    # ...
    dict={"name":name,"age":age}
    print(dict)
    return dict

if __name__ == '__main__':
    app.run()

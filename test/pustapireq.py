import requests

api_url = 'http://127.0.0.1:5000/'  # Replace with your API endpoint URL
data = {
    'name': 'abc',
    'age': '23'
}

try:
    response = requests.post(api_url, json=data)
    response.raise_for_status()  # Raise an exception for any HTTP errors

    # Process the API response
    print(response.json())

except requests.exceptions.RequestException as e:
    print(f"Error occurred: {e}")

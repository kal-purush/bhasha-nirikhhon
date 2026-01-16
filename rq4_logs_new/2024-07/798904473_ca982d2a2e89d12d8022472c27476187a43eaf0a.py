import requests
from requests.auth import HTTPBasicAuth


SHEET_API_ENDPOINT = ""


class DataManager:

    def __init__(self):
        self.user = ""
        self.password = ""
        self.authorization = HTTPBasicAuth(self.user, self.password)
        self.destination_data = []

    def get_destination_data(self):
        response = requests.get(url=SHEET_API_ENDPOINT,
                                auth=(self.authorization.username, self.authorization.password))
        response.raise_for_status()
        data = response.json()
        self.destination_data = data["prices"]
        return self.destination_data

    def update_destination_code(self):
        for city in self.destination_data:
            new_data = {
                "price": {
                    "iataCode": city["iataCode"]
                }
            }
            response = requests.put(
                url=f"{SHEET_API_ENDPOINT}/{city["id"]}",
                auth=(self.authorization.username, self.authorization.password),
                json=new_data
            )
            response.raise_for_status()  # This will raise an exception for HTTP errors
            print(f"Updated {city['city']}: {response.status_code}")

            print(response.text)

    def update_price(self, row_id, new_price):
        new_data = {
            "price": {
                "lowestPrice": new_price
            }
        }
        response = requests.put(
            url=f"{SHEET_API_ENDPOINT}/{row_id}",
            auth=(self.authorization.username, self.authorization.password),
            json=new_data
        )
        response.raise_for_status()
        print(f"Updated price for row {row_id}: {response.status_code}")
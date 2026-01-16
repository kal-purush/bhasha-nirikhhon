import requests
from datetime import datetime

GENDER = "male"
WEIGHT_KG = 67
HEIGHT_CM = 172
AGE = 37

# Nutritionix API
APP_ID = ""
API_KEY = ""
exercise_endpoint = ""
sheet_endpoint = ""

NUT_QUERY = input("Tell me which exercises you did: ")

header = {
    "x-app-id": APP_ID,
    "x-app-key": API_KEY
}

parameters = {
    "query": NUT_QUERY,
    "gender": GENDER,
    "weight_kg": WEIGHT_KG,
    "height_cm": HEIGHT_CM,
    "age": AGE
}

response = requests.post(url=exercise_endpoint, json=parameters, headers=header)
result = response.json()


################### Start of Step 4 Solution ######################
# Sheety API
today_date = datetime.now().strftime("%d/%m/%Y")
now_time = datetime.now().strftime("%X")

sh_header = {
    "Authorization": ""
}
for exercise in result["exercises"]:
    sheet_inputs = {
        "workout": {
            "date": today_date,
            "time": now_time,
            "exercise": exercise["name"].title(),
            "duration": exercise["duration_min"],
            "calories": exercise["nf_calories"]
        }
    }

    sheet_response = requests.post(sheet_endpoint, json=sheet_inputs, headers=sh_header)

    print(sheet_response.text)
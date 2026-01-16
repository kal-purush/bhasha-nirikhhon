import requests
import time
import csv

ids_of_crypto = ["ethereum", "cardano", "solana"]


def get_crypto(cryptocurrency):
    end_time = "00:00"
    with open("crypto_prices.csv", "w", newline='') as file:
        data = ["Time", *[cr.title() for cr in ids_of_crypto]]
        writer = csv.writer(file, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(data)

        while time.strftime("%H:%M") != end_time:
            row_data = [time.strftime("%H:%M")]
            for crypto in cryptocurrency:
                response = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={crypto}&vs_currencies=usd")
                response.raise_for_status()
                price = response.json()[crypto]["usd"]
                row_data.append(f"{price}$")
            writer.writerow(row_data)
            time.sleep(60)


get_crypto(ids_of_crypto)
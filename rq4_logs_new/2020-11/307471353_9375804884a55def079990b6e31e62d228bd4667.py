from exchangeratesapi import Api

api = Api()

# Get the latest foreign exchange rates:
print(api.get_rates('USD'))
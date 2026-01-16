from googleapiclient.discovery import build

api_key='AIzaSyAqSF-Y2Ad2ZHAt5tSeaKowo8qfT8uyZwM'

youtube=build('youtube','v3',developerKey=api_key)

request=youtube.channels().list(part='statistics',forUsername='sentdex')

response=request.execute()

print(response)
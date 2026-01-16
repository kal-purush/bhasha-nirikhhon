import requests
def get_quote():
	url = 'https://api.quotable.io/random'
	output = ''

	r = requests.get(url)
	quote = r.json()
	output += quote['content'] + '\n'+f"\t-{quote['author']}"

	return output
print(get_quote())
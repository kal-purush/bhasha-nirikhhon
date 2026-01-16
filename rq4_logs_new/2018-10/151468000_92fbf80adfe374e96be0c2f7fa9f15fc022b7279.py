import requests

address = input("Address DOGE   : ")

response_balance = requests.get('https://dogechain.info/api/v1/address/balance/'+ address +'')
response_received = requests.get('https://dogechain.info/api/v1/address/received/'+ address +'')
response_sent = requests.get('https://dogechain.info/api/v1/address/sent/'+ address +'')
response_unspent = requests.get('https://dogechain.info/api/v1/unspent/'+ address +'')

if response_balance.status_code == 200:
	content_balance = response_balance.json()

if response_received.status_code == 200:
	content_received = response_received.json()

if response_sent.status_code == 200:
	content_sent = response_sent.json()

if response_unspent.status_code == 200:
	content_unspent = response_unspent.json()
	unspent_content = content_unspent.get('unspent_outputs')

	print("---------------")
	print("Address Info")
	print("---------------")
	print("Balance        :", content_balance['balance'],"  DOGE")
	print("Total Received :", content_received['received'],"DOGE")
	print("Total Sent     :", content_sent['sent'],"DOGE")
	print("\n")
	print("---------------")
	print("Unspent Outputs")
	print("---------------")

	for n, unspent in enumerate(unspent_content):
		tx_hash = unspent['tx_hash']
		tx_output_n = unspent['tx_output_n']
		script = unspent['script']
		value = unspent['value']
		confirmations = unspent['confirmations']
		address = unspent['address']

		print(n)
		print("TX Hash       :", tx_hash)
		print("TX Outpu      :", tx_output_n)
		print("Script        :", script)
		print("Value         :", value)
		print("Confirmations :", confirmations)
		print("Address       :", address)
		print("\n")
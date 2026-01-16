import censys.certificates
import csv
from settings import UID, SECRET

certificates = censys.certificates.CensysCertificates(UID, SECRET)
fields = ["parsed.fingerprint_sha256", "parsed.validity.start", "parsed.validity.end"]

with open('results.csv', 'w', newline='') as file:
	writer = csv.writer(file)
	writer.writerow(["SHA256 fingerprint", "validity start date", "validity end date"])

	for c in certificates.search("parsed.names: censys.io, tags: trusted", fields=fields):
		writer.writerow([c["parsed.fingerprint_sha256"], c["parsed.validity.start"], c["parsed.validity.end"]])

# for c in certificates.search("parsed.names: censys.io, tags: trusted", fields=fields):
# 	print(c["parsed.validity.start"] + " " + c["parsed.fingerprint_sha256"] + " " + str(c["parsed.validity.end"]))
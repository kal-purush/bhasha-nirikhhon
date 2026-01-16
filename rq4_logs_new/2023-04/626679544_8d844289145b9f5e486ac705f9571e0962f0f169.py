from fastapi import FastAPI
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import schedule
import time

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    while True:
        schedule.run_pending()
        time.sleep(1)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/query")
async def query(query: str):
    # code for checking ebay kleinanzeigen listings and sending email
    return {"message": f"Query received: {query}"}


def send_email(listings):
    sender = 'sm.bokhari@hof-university.de'
    receiver = 'mushtaq96smb@gmail.com'
    password = 'Baaziger1996mus'

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = receiver
    msg['Subject'] = 'New Ebay Kleinanzeigen Listing Found'

    body = 'New listing found. Please check the link below: ' + \
        '\n'.join(listings)
    msg.attach(MIMEText(body, 'plain'))

    smtp_server = 'outlook.hof-university.de'
    smtp_port = 25
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(sender, password)
    text = msg.as_string()
    server.sendmail(sender, receiver, text)
    server.quit()


def check_for_new_listings():

    # Nimmt sich das momentane Datum & formatiert es zu '[Stunde:Minute:Sekunde] - '. / Takes the current date & formats it to '[Hour:Minute:Second] - '.
    now = datetime.now()
    prefix = "[" + now.strftime("%H:%M:%S") + "] - "

    # Eine Funktion um eine Text-Datei zu beschreiben. / A function to write to a text file.

    def writeFile(response, name, fileType):
        with open(name + fileType, "a", encoding="utf8") as f:
            f.write(response)

    # Eine Funktion um den Durschnitt zu berechnen. / A function to calculate the average.

    def calculateAverage(Sum, count):
        return Sum / count

    # Eine Funktion um die Summe eines Arrays zu berechnen. / A function to calculate the sum of an array.

    def calculateSum(arr):
        result = sum(arr)
        return result

    # Fragt die Query ab die Später in den URL gegeben werden soll (der Suchbegriff) / Asks for the query that will be later inserted into the URL (the search term)
    query = input(prefix + "Bitte gebe deine Query an << ")

    # Fügt die Query in den Ebay-Kleinanzeigen URL ein. / Inserts the query into the Ebay-Kleinanzeigen URL.
    # URL = "https://www.ebay-kleinanzeigen.de/stadt/stuttgart/" + query + "/k0l9280"
    URL = "https://www.ebay-kleinanzeigen.de/s-zu-verschenken/stuttgart/" + \
        query + "/k0c192l9280"
    # Gibt aus welche Query gewählt wurde. / Outputs which query was chosen.
    print(prefix + "Es wird nach gesucht nach >> " + query)

    # Setzt die Headers der Anfrage (Den User-Agent), damit Ebay-Kleinanzeigen die Anfrage nicht blockt. / Sets the headers of the request (the User-Agent) so that Ebay-Kleinanzeigen does not block the request.
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.125 Safari/537.36 Edg/84.0.522.59',
    }

    # Gibt den HTML text der Website in eine Variable wieder. / Puts the HTML text of the website into a variable again.
    response = requests.get(url=URL, headers=headers)

    # Setzt den Content der Website in eine Variable. / Sets the content of the website into a variable.
    page = response.content

    # Erstellt eine BeautifulSoup-Instanz mit dem Website-Content und einem passendem Parser. / Creates a BeautifulSoup instance with the website content and a suitable parser.
    soup = BeautifulSoup(page, "html.parser")
    # Setzt die Search-Results-Content in eine Variable. / Sets the search results content into a variable.
    srchRsltsContent = soup.find("div", id="srchrslt-content")

    # Setzt die Einträge inheralb der Seach-Results und dem Table dortdrinn in eine Variable / Array. / Sets the entries within the search results and the table therein into a variable/array.
    srchRslts = soup.find_all("li")

    # Setzt einen Counter. / Sets a counter.
    counter = 0

    # Setzt einen zweiten Counter. / Sets a second counter for 'VB' listings.
    vbCounter = 0
    ePreise = []

    count = 0
    url_list = []
    baseURL = 'https://www.ebay-kleinanzeigen.de'
    result = soup.find_all('ul', {'id': 'srchrslt-adtable'})
    for ul in result:
        li_elements = ul.find_all('li', {'class': 'ad-listitem lazyload-item'})
        # print(li_elements)
        for li in li_elements:
            link = li.find('a')
            if link:
                href = link.get('href')

                if href and '/s-anzeige/' in href:
                    url_list.append(baseURL + href)
                    # print(href)
                    count = count + 1
    print(len(url_list))
    if len(url_list) > 0:
        send_email(url_list)

# Looped durch alle Search-Results durch. / Loops through all search results.
    for srchRslt in srchRslts:
        # print(srchRslt.get_text())

        # Nimmt sich alles mit dem <strong> tag. / Takes everything with the <strong> tag.
        preise = srchRslt.find_all(
            'p', {'class': 'aditem-main--middle--price-shipping--price'})

        # Geht durch jedes Element mit dem <strong> tag durch. / Goes through each element with the <strong> tag.
        for preis in preise:
            # Incrementiert einen Counter. / Increments a counter.
            counter = counter + 1

            # Gibt den Preis des Listings aus & fügt den Incrementierten Counter hinzu (+ formatierung). / Outputs the price of the listing & adds the incremented counter (+ formatting).
            price = preis.text.strip().split(" ")[0]
            print("#" + str(counter) + " | " + price)
            price_float = float(price)
            if price_float < 20:
                print("Preis ist unter 20€")

            # Zählt wieviele Listings mit 'VB' gekennzeichnet sind. / Counts how many listings are marked with 'VB'.
            if ("VB" in preis.text):
                vbCounter = vbCounter + 1

            # Entfernt Chars, wenn das Euro-Zeichen vorhande ist. / Removes chars if the euro symbol is present.
            # Konvertiert außerdem den Preis in eine Int und speichert ihn in den Array 'ePreise'. / Also converts the price to an int and stores it in the 'ePreise' array.
            if ("€" in preis.text):
                if ("." in preis.text):
                    if ("VB" in preis.text):
                        ePreise.append(int(preis.text.replace(" ", "").replace(
                            "VB", "").replace("€", "").replace(".", "")))
                    else:
                        ePreise.append(int(preis.text.replace(
                            " ", "").replace("€", "").replace(".", "")))
                    ePreise.append(int(preis.text.replace(" ", "").replace(
                        "VB", "").replace("€", "").replace(".", "")))
                else:
                    ePreise.append(int(preis.text.replace(
                        " ", "").replace("VB", "").replace("€", "")))
            else:
                if ("€" in preis.text):
                    if ("." in preis.text):
                        ePreise.append(int(preis.text.replace(
                            " ", "").replace("€", "").replace(".", "")))
                    else:
                        ePreise.append(
                            int(preis.text.replace(" ", "").replace("€", "")))

    # Gibt verhandelbare Listings aus. / Outputs negotiable listings.
    print(prefix + "Verhandelbare Listings >> " + str(vbCounter))

    # Gibt die Anzahl aller Listings aus. / Outputs the number of all listings.
    print(prefix + "Anzahl der Listings >> " + str(counter))

    # Gibt den Durschnitspreis der Listings aus. / Outputs the average price of the listings.
    print(prefix + "Durschnitts-Preis der Listings >> " +
          str(calculateAverage(calculateSum(ePreise), counter) if counter > 0 else 0))


schedule.every(1).hour.do(check_for_new_listings)
# create a schedule to run every 2 minutes
schedule.every(2).minutes.do(check_for_new_listings)
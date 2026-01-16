#!/usr/bin/env python3
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
from bs4 import BeautifulSoup


html = requests.get(
    "https://www.amctheatres.com/movie-theatres/phoenix/amc-ahwatukee-24")
soup = BeautifulSoup(html.text, "html.parser")
movie_titles = soup.find_all("div", class_="Slide")
all_children = list(
    map(lambda x: x.contents[1].contents[0].contents[0].get_text(), movie_titles))
filtered_movies = list(dict.fromkeys(all_children))
generated_html = list(map(lambda x: f'<li>{x}</li>', filtered_movies))
joined = " ".join(generated_html)

html_string = """\
<html>
  <body>
  <h2>Here is what is in theatres this week:</h2>
    <ul>
      {generated_html}
    </ul>
  </body>
</html>
""".format(generated_html=joined)

generated_html = MIMEText(html_string, "html")

sender_email = "etokatlian@gmail.com"
receiver_email = "etokatlian@gmail.com"
password = ''
message = MIMEMultipart("alternative")
message["Subject"] = "This weeks movie briefing"
message["From"] = sender_email
message["To"] = receiver_email
message.attach(generated_html)

context = ssl._create_unverified_context()

with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(
        sender_email, receiver_email, message.as_string()
    )
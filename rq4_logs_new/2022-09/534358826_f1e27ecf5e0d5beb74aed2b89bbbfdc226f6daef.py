#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO

Write Docstring.
"""

__author__ = "Martin Pucovski"
__copyright__ = "Copyright 2022, Martin Pucovski"
__credits__ = ["Martin Pucovski"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Martin Pucovski"
__email__ = "martin@pucov.ski"
__status__ = "Production"

import configparser
import datetime
import logging
import smtplib
import secrets
from email.mime.text import MIMEText
import csv
import random


# Create and configure logger
current_day = datetime.datetime.now().strftime("%Y%m%d")

logging.basicConfig(filename=f"logs\{current_day}_log.log",
                    format='%(asctime)s %(levelname)s %(message)s',
                    filemode='a')

# Creating an object
logger = logging.getLogger()

# Setting the threshold of logger
logger.setLevel(logging.INFO)

logger.info("####################")
logger.info("Script started")

# read config.ini file
logger.info("Reading config")
config = configparser.ConfigParser()
config.read('config\config.ini')
config_default = config['DEFAULT']


def send_mail(mail_message: str) -> None:
    """
    Module to send mail message

    :param mail_message: text of the mail message
    :returns: None
    """
    sender = secrets.smtp_username
    receivers = [secrets.recipient]

    msg = MIMEText(mail_message)
    msg['Subject'] = "Quote of the day"
    msg['From'] = sender
    msg['To'] = ",".join(receivers)

    server = smtplib.SMTP_SSL(secrets.smtp_server, secrets.smtp_port)
    server.login(secrets.smtp_username, secrets.smtp_password)
    server.sendmail(sender, receivers, msg.as_string())
    server.quit()


def main():
    # read file with quotes
    with open(r'data/all_quotes.csv', newline='', encoding="utf8") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=';')
        header = []
        header = next(spamreader)
        rows = []
        for row in spamreader:
            rows.append(row)
        
    # choose one random quote
    number_of_quotes = len(rows)
    value = random.randrange(0, number_of_quotes-1)

    # build text
    with open(r'data/mail_template.txt', newline='', encoding="utf8") as mail_template:
        template = mail_template.read()
    
    mail_message = template.format(rows[value][0], rows[value][1], rows[value][2], rows[value][3])

    # send mail with
    send_mail(mail_message)

if __name__ == "__main__":
    main()


logger.info("Script ended")
logger.info("####################")
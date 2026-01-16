#!/usr/bin/env python3

# Copyright (c) 2024, Mike - KnightCraftDev

# plugins/plugin_readexchangetask.py
# Dieses Plugin liest alle bevorstehenden Aufgaben aus dem Exchange-Server aus.

import sys
import pytz
from plugin_interface import PluginInterface
from datetime import datetime, timedelta
from itertools import chain

from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from exchangelib import DELEGATE, IMPERSONATION, Account, Credentials, \
    EWSDateTime, EWSTimeZone, Configuration, NTLM, CalendarItem, Message, \
    Mailbox, Attendee, Q, ExtendedProperty, FileAttachment, ItemAttachment, \
    HTMLBody, Build, Version, FolderCollection
from cryptography.fernet import Fernet
from datetime import datetime, timedelta
from exchangelib.errors import UnauthorizedError
from exchangelib.winzone import MS_TIMEZONE_TO_IANA_MAP, CLDR_TO_MS_TIMEZONE_MAP

from main import check_timezone

# MS_TIMEZONE_TO_IANA_MAP = {
#    'Pacific Standard Time': 'America/Los_Angeles',
#    'Eastern Standard Time': 'America/New_York',
#    'Central European Standard Time': 'Europe/Berlin',
#    '': 'Europe/Berlin'
#    # Fügen Sie hier weitere Zuordnungen hinzu
# }

# Beispielhafte Zuordnung von CLDR-Zeitzonen zu Microsoft-Zeitzonen
CLDR_TO_MS_TIMEZONE_MAP = {
    'America/Los_Angeles': 'Pacific Standard Time',
    'America/New_York': 'Eastern Standard Time',
    'Europe/Berlin': 'Central European Standard Time',
    # Fügen Sie hier weitere Zuordnungen hinzu
}


class Sender:
    def __init__(self):
        self.name = None
        self.email = None


class EmailData:
    def __init__(self):
        self.sender = Sender()
        self.subject = ""
        self.body = ""


class PluginReadExchangeTask(PluginInterface):

    # Class-Variable
    start_date = None
    end_date = None
    account = None
    modulName = None
    plugin_config = None
    DaysAhead = 4  # Default-Wert für die Anzahl der Tage, die im Voraus nach Aufgaben gesucht werden sollen
    timeZone = "Europe/Berlin"  # Default-Wert für die Zeitzone
    additional_mailboxes = []  # Liste mit zusätzlichen Postfächern

    def __init__(self, config):
        super().__init__(config)  # Initialisierung der Basisklasse
        self.modulName = self.__module__.split('.')[-1]
        _additional_accounts = []

        if 'timeZone' in config:
            if not check_timezone(config['timeZone']):
                print(
                    f"Die angegebene Zeitzone ist ungültig. Verwende die Standardzeitzone: {timeZone}")
            else:
                self.timeZone = config['timeZone']
        if 'additional_mailboxes' in config:
            # Liste der zusätzlichen Postfächer
            _additional_accounts = config['additional_mailboxes']
            if not isinstance(_additional_accounts, list):
                _additional_accounts = []

        if 'plugin_' + self.modulName in config:
            plugin_config = config['plugin_' + self.modulName]
            if isinstance(plugin_config, dict):
                self.plugin_config = plugin_config
            else:
                self.plugin_config = {
                    'DaysAhead': 4, 'timeZone': self.timeZone, 'access_additional_mailboxes': False}

        self.DaysAhead = self.plugin_config.get('DaysAhead', 4)

        self.timeZone = self.plugin_config.get('timeZone', 'Europe/Berlin')
        MS_TIMEZONE_TO_IANA_MAP[''] = self.timeZone

        self.start_date = datetime.now(pytz.timezone(self.timeZone))
        self.end_date = self.start_date + timedelta(days=self.DaysAhead)
        self.end_date = self.end_date.replace(hour=23, minute=59, second=59)

        tmp_add = self.plugin_config.get('access_additional_mailboxes', False)
        if isinstance(tmp_add, str):
            tmp_add = tmp_add.lower()
        if tmp_add in [True, 1, "true", "yes", "ja"]:
            self.additional_mailboxes = _additional_accounts

        print(self.end_date)

    def add_emails(self, unread_emails, unread_count):
        unread_emails, unread_count = self.read_personal_calendar(
            unread_emails, unread_count)
        return unread_emails, unread_count

    def get_all_calendars(self, account):
        calendars = [account.calendar]
        for folder in account.calendar.children:
            if folder.folder_class == 'IPF.Appointment':
                calendars.append(folder)
        return calendars

    # Persönliche Kalenderdaten auslesen

    def read_personal_calendar(self, unread_emails, unread_count):
        # Authentifizierung
        credentials = Credentials(
            username=self.config.get('username', ''), password=self.config.get('password', ''))
        exchange_config = Configuration(
            server=self.config.get('server', ''),
            credentials=credentials,
            auth_type=NTLM
        )

        # Zugriff auf das Postfach
        try:
            self.account = Account(
                primary_smtp_address=self.config.get('email', ''),
                config=exchange_config,
                autodiscover=False,
                access_type=DELEGATE
            )

            # for cal_folder in self.account.calendar.children:
            #    print(cal_folder)

        except UnauthorizedError:
            print("Fehler: Ungültige Anmeldedaten. Das geheime Passwort wurde gelöscht (password.enc und secret.key).")

            sys.exit(1)  # Programm beenden
        except Exception as e:
            print(f"Ein Fehler ist aufgetreten: {e}")
            sys.exit(1)  # Programm beenden

        print("Hauptaccount Kalender ausgelesen.")
        unread_emails, unread_count = self.read_calendarByAccount(
            self.account, unread_emails, unread_count)

        # Zusätzliche Postfächer auslesen
        for additional_mailbox in self.additional_mailboxes:
            print(f"Zusätzliches Postfach: {additional_mailbox}")
            try:
                account_additional = Account(
                    primary_smtp_address=additional_mailbox,
                    config=exchange_config,
                    autodiscover=False,
                    access_type=DELEGATE
                )
                unread_emails, unread_count = self.read_calendarByAccount(
                    account_additional, unread_emails, unread_count)
                # Verbindung trennen
                account_additional.protocol.close()
            except Exception as e:
                print(f"Ein Fehler ist aufgetreten: {e}")
                sys.exit(1)

        # Verbindung trennen
        self.account.protocol.close()

        return unread_emails, unread_count

    def read_calendarByAccount(self, account, unread_emails, unread_count):

        # Alle Kalender abrufen (Hauptkalender und untergeordnete Kalender)
        all_calendars = self.get_all_calendars(self.account)

        # FolderCollection für alle Kalender erstellen
        calendar_collection = FolderCollection(
            account=self.account, folders=all_calendars)
        # Alle Ereignisse im Zeitraum für alle Kalender abrufen
        events = calendar_collection.view(
            start=self.start_date, end=self.end_date)

        # Ereignisse ausgeben
        for event in events:
            # print(f"Kalender: {event.folder.name}")

            # Start- und Endzeit in lokaler Zeitzone anzeigen
            start_local = event.start.astimezone(pytz.timezone(self.timeZone))
            stop_local = event.end.astimezone(pytz.timezone(self.timeZone))

            # Datum und Uhrzeit formatieren
            start_local = start_local.strftime("%d.%m.%Y %H:%M")
            stop_local = stop_local.strftime("%d.%m.%Y %H:%M")

            # Definiere data als class
            data = EmailData()

            data.sender.name = f"EVENT {start_local}"
            data.sender.email = "EVENT"
            data.subject = event.subject
            data.body = f"Ist wiederkehrend: {event.is_recurring}"

            unread_emails.append(data)
            unread_count += 1

            print(f"Betreff: {event.subject}")
            print(f"Start: {start_local}")
            print(f"Ende: {stop_local}")
            print(f"Ist wiederkehrend: {event.is_recurring}")
            if event.is_recurring:
                print(f"Wiederholungsmuster: {event.recurrence}")
            print("------------------------")

        return unread_emails, unread_count


# Verhindern, dass das Plugin alleine ausgeführt wird
if __name__ == "__main__":
    print("Dieses Plugin kann nicht alleine ausgeführt werden. Bitte führen Sie das Hauptprogramm aus.")
    sys.exit(1)
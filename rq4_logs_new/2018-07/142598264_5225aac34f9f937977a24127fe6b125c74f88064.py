from google.oauth2 import service_account
from googleapiclient import discovery


class SheetsImporter:

    def __init__(self, cred_file):
        """
        Initialise class with a path to credential file to set up
        Google DRIVE and SHEETS API services
        """

        scopes = [
            'https://www.googleapis.com/auth/drive.readonly.metadata',
            'https://www.googleapis.com/auth/spreadsheets'
        ]
        service_account_key_file = cred_file

        credentials = service_account.Credentials.from_service_account_file(
            service_account_key_file, scopes=scopes)

        drive_service = discovery.build('drive', 'v3', credentials=credentials)
        sheets_service = discovery.build('sheets', 'v4', credentials=credentials)

        self.services = {
            'sheets_service': sheets_service,
            'drive_service': drive_service
        }

    def get_google_sheet_files(self, drive_file = None):
        """
        List all files in DRIVE belonging to the service account which are
        Google Sheet files
        :param drive_file the DRIVE folder to check for google sheets files
        TODO : implement checking in the provided google drive folder for sheets files
        """
        drive_service = self.services['drive_service']
        files = drive_service.files().list().execute().get('files', [])
        sheet_files = []
        for f in files:
            # pick sheet files by checking mime types
            if f['mimeType'] == 'application/vnd.google-apps.spreadsheet':
                sheet_files.append(f)
        return sheet_files

    def get_sheet_range_output(self, sheet_file, range):
        """
        Get the values in the range of the provided sheets file
        :param sheet_file: the file whose content is to be picked
        :param range: the range of columns and rows whose values are to be returned
        :return: a list of list of values in the range specified
        """
        sheets_service = self.services['sheets_service']
        result = sheets_service.spreadsheets().values().get(spreadsheetId=sheet_file['id'], range=range).execute()
        values = result.get('values', [])
        return values

    def list_sheet_contents(self, sheet_files, range):
        """
        List contents of files with the range provided
        :param sheet_files:
        :param range: rows and columns to be returned
        :return:
        """
        for file in sheet_files:
            values = self.get_sheet_range_output(file, range)
            print("File : " + file['name'])
            for row in values:
                print(row)


if __name__ == '__main__':

    g_cred_file = '/Users/solomon/CHEWSE/sheets_import/credential_key.json'

    sheets_importer = SheetsImporter(cred_file)
    g_sheet_files = sheets_importer.get_google_sheet_files()
    sheets_importer.list_sheet_contents(g_sheet_files, "Sheet1")







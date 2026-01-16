import { google, drive_v3, sheets_v4 } from 'googleapis';
import { Memoize } from 'typescript-memoize';
import { IJournal } from './IJournal';

export class GoogleDriveDocWriter implements IJournal {
    private static readonly folderName: string = "alexa-dream-journal";
    private drive: drive_v3.Drive;

    public constructor(accessToken: string) {
        const oauth2Client = new google.auth.OAuth2();
        oauth2Client.setCredentials({ access_token: accessToken });
        this.drive = google.drive({
            version: 'v3',
            auth: oauth2Client
        });
    }

    public async createEntry(contents: string): Promise<void> {
        const folderId = await this.getOrCreateFolder();
        await this.drive.files.create({
            requestBody: {
                name: 'DreamSkill',
                mimeType: 'text/plain',
                parents: [folderId]
            },
            media: {
                mediaType: 'text/plain',
                body: contents
            }
        }, null);
    }

    private async getOrCreateFile(fileName: string) {
        const fileType = 'text/plain';
        const parentFolder = await this.getOrCreateFolder();
        const fileId = await this.getItem(fileType, fileName, parentFolder);

        if (fileId) {
            return fileId;
        }

        const res = await this.drive.files.create({
            requestBody: {
                name: fileName,
                mimeType: fileType,
                parents: [parentFolder]
            }
        }, null);

        return res.data.id;
    }

    private async getItem(mimeType: string, name: string, parent?: string): Promise<string | null> {
        const queryParts: string[] = [`name=${name}`, `mimeType=${mimeType}`];
        if (parent) {
            queryParts.push(`'${parent}' in parents`);
        }
        const queryJoin = "and";

        const getItemRes = await this.drive.files.list({
            q: queryParts.join(queryJoin)
        }, null);
        const itemId = getItemRes.data.files && getItemRes.data.files[0].id;
        if (!itemId) {
            return null;
        }

        return itemId;
    }

    @Memoize()
    private async getOrCreateFolder(): Promise<string> {
        const folderType = 'application/vnd.google-apps.folder';
        const folderId = await this.getItem(folderType, GoogleDriveDocWriter.folderName);
        if (folderId) {
            return folderId;
        }

        // otherwise, create a new folder
        const res = await this.drive.files.create({
            requestBody: {
                name: GoogleDriveDocWriter.folderName,
                mimeType: folderType
            }
        }, null);

        return res.data.id;
    }
}

export class GoogleDriveSpreadsheetWriter implements IJournal {
    private static readonly spreadsheetName: "alexa-dream-journal";
    private sheets: sheets_v4.Sheets;

    public constructor(accessToken: string) {
        const oauth2Client = new google.auth.OAuth2();
        oauth2Client.setCredentials({ access_token: accessToken });
        this.sheets = google.sheets({
            version: 'v4',
            auth: oauth2Client
        });
    }

    public async createEntry(contents: string): Promise<void> {
        const spreadsheet = await this.getOrCreateSpreadSheet();
        const entryTime: string = new Date().toLocaleString();
        const entry = [entryTime, contents];

        this.sheets.spreadsheets.values.append({
            spreadsheetId: spreadsheet.spreadsheetId,
            valueInputOption: "RAW",
            insertDataOption: "INSERT_ROWS",
            requestBody: {
                values: [entry]
            }
        })
    }

    private async getOrCreateSpreadSheet(): Promise<sheets_v4.Schema$Spreadsheet> {
        const response = await this.sheets.spreadsheets.create({
            requestBody: {
                properties: {
                    title: GoogleDriveSpreadsheetWriter.spreadsheetName
                }
            }
        });

        return response.data;
    }
}
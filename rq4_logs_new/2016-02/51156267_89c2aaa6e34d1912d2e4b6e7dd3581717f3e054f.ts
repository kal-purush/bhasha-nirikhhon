module jw.MaterialsTracker
{
    export class Server
    {
        lookupProjectFromHash = (hash: string): jw.MaterialsTracker.Interfaces.IProjectHashLookupResponse =>
        {
            var hashLookupSsid: string = jw.MaterialsTracker.Config.ConfigurationManager.getSetting(jw.MaterialsTracker.Config.ConfigurationManager.projectNumberLookupSsidKey);

            //Open the spreadsheet using the ssid
            var hashLookupSs: GoogleAppsScript.Spreadsheet.Spreadsheet = SpreadsheetApp.openById(hashLookupSsid);

            var sheet: GoogleAppsScript.Spreadsheet.Sheet = hashLookupSs.getSheets()[0];

            var range: GoogleAppsScript.Spreadsheet.Range = sheet.getRange(2, 1, 100, 2);

            var projHash: Object[] = jw.MaterialsTracker.Utilities.RangeUtilties.findFirstRowMatchingKey(range, hash);

            var response: jw.MaterialsTracker.Interfaces.IProjectHashLookupResponse = {
                projectNumber: parseInt(projHash[1].toString()),
                urlHash: projHash[0].toString()
            };

            return response;
        };
    }
}


var hash: string;

function doGet(request: GoogleAppsScript.Script.IParameters)
{
    hash = request.parameter['projHash'];

    var t: GoogleAppsScript.HTML.HtmlTemplate = HtmlService.createTemplateFromFile('Hash');

    t.data = hash;    

    return t.evaluate().setSandboxMode(HtmlService.SandboxMode.IFRAME);
}
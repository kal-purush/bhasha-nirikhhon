var UI = null;

function onOpen(e) {
    UI = SpreadsheetApp.getUi();

    var statusUpdateSubMenu =
        UI.createMenu('Update Order Status')
            .addItem('...to Sent', 'updateStatustoSent')

  UI.createMenu('Purchasing Tools')
        .addItem('Show Purchasing Sidebar', 'purchasingSidebar')
        .addSeparator()
        .addSubMenu(statusUpdateSubMenu)
        .addSeparator()
        .addItem('Clean Up Formats, Formulae and Drop-downs', 'matCleanup')
        .addToUi();
    setupCLPicker();
    setupGoodsReceiver();
    matDropDowns();
}

/*
* Settings for the purchasing side bar
*/
function purchasingSidebar() {
    UI = SpreadsheetApp.getUi();
    var html = HtmlService.createHtmlOutputFromFile('SideBarPlain')
        .setSandboxMode(HtmlService.SandboxMode.IFRAME)
        .setTitle('Purchasing Side Bar')
        .setWidth(250);
    UI.showSidebar(html);
}

/**
 * Runs when the add-on is installed; calls opened() to ensure menu creation and
 * any other initializion work is done immediately.
 *
 * @param {Object} e The event parameter for a simple onInstall trigger.
 */
function onInstall(e) {
    onOpen(e);
}

function edited(e) {
    Logger.log("Cell Edited = " + SpreadsheetApp.getActiveSpreadsheet().getActiveSheet().getActiveCell().getA1Notation());
    UI = SpreadsheetApp.getUi();
    SH = SS.getActiveSheet();
    var SHname = SH.getName();
    thisCell = SH.getActiveCell();

    if (SHname == CORELIST_SHEET) {
        clEdit(e);
    }

    if (SHname == MATERIALS_SHEET) {
        matEdit(e);
    }

    if (SHname == GOODS_RECEIVING_SHEET) {
        grEdit(e);
    }

    if (SHname == SVS_MATCHER_SHEET) {
        svsEdit(e);
    }

    if (SHname == NONCORE_SHEET) {
        ncEdit(e);
    }

} // end fn:onEdit









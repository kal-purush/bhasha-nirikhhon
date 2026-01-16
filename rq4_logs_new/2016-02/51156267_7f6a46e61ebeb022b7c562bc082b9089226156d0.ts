/**
 * Functions in this code file are:
 * onOpen
 * purchasingSidebar
 * onInstall
 * edited
 * clearCache
 * cacheUpdater
 * putCache
 * getCache
 * getCL
 * getWTSup
 * getCentralDropDowns
 * setupCLPicker
 * clEdit
 * setCLFilters
 * displayFilteredCLList
 * checkForDuplicates
 * filterCLList
 * setCLDropDown
 * basketList
 * sendBasket
 * searchCLbyIC
 * clearCells
 * setupGoodsReceiver
 * getML
 * grEdit
 * setGRFilters
 * clearDisplayGRList
 * compileFilteredGRList
 * filterGRList
 * setGRDropDown
 * markGoodsReceived
 */

/*
 * Global scope variables
 */
var UI = null;
var CL = null; //out of script holder for the CL return to speed up cache calls

var clHeaderRow = 6;
var ddPDN = 'C2';
var ddTrade = 'C3';
var ddSubcategory = 'D3';
var ddType = 'E3';
var ddLocation = 'H3';
var ddPackage = 'C4'; /* WIP */
var ddTeamRequested = 'G3';
var ddSendBasket = 'J3';
var ddViewAll = 'A2';

var dupWarningCell = 'A4';
var dupResultsCell = 'B4';

// sort options
var clSortRow = 5;

// headers as in Central Data Core List for filtering
var clSubcategory = 'Product sub category';
var clType = 'Type';
var clLocation = 'Location';
var clPackage = 'Package key';
var clPDC = 'PDC';

// other headers from Central Data Core List
var clTrade = 'Trade';
var clIC = 'Item Code';
var clID = 'Item Description';
var clLastPrice = 'Expected Purchase Price';
var clPUoM = 'Purchase UOM';
var clFactor = 'Factor';
var clBUoM = 'Base UOM';
var clBrand = 'Brand';
var clMfg = 'Manufacturer';
var clPartNo = 'Part Number';
var clItemStatus = 'Status';
var clInactive = 'Inactive from';
var clReplacement = 'Replacement';
var clDataLink = 'Data Link';

// headers from basket
var bIC = 'Item Code';
var bID = 'Item Description';
var bMfg = 'Manufacturer';
var bBrand = 'Brand';
var bPartNo = 'Part Number';
var bLastPrice = 'Last Price';
var bQty = 'Quantity Requested';
var bPUoM = 'Purchase UoM';
var bFactor = 'Factor';
var bBUoM = 'Base UoM';
var bPDN = 'Budget PDC';
var bUsage = 'Usage';

var grHeaderRow = 6;
var grSortRow = 5;
var ddPurchasingRoute = 'F3';
var supplierColNum = 6;
var mfgColNum = 4;
var partColNum = 5;
var grLastUpdated = 'G3';
var ddMarkReceived = 'L3';

var grCOREopt = 'Core';
var grWTopt = WT_PREFIX;
var grSHOWALLopt = 'GO!';

var MATLIST = null;

/*
 * End global scope variables
 */

/*
* Functions originally in Code.gs
*/

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

/*
 * End function originally in Code.gs
 */


/*
 * Functions originally in CD Cache.gs
 */
function clearCache() {
    CacheService.getDocumentCache().removeAll(['CDdd', CD_CoreList, CD_WTSuppliers]);
}

function cacheUpdater() {

    getCentralDropDowns();

    var caches = [CD_CoreList, CD_WTSuppliers];

    // store current spreadsheet settings
    var origSS = SpreadsheetApp.getActiveSpreadsheet();
    var origSH = origSS.getActiveSheet();
    var origRange = origSH.getActiveRange();

    for (; caches.length > 0;) {
        var cacheKey = caches.pop();
        if (CacheService.getDocumentCache().get(cacheKey) == null) {
            putCache(cacheKey, cacheKey);
        } // end if cache is empty
    } // end for loop

    // return to the original spreadsheet
    SpreadsheetApp.setActiveSpreadsheet(origSS);
    SpreadsheetApp.setActiveSheet(origSH).setActiveRange(origRange);

} // end fn:cacheUpdater

function putCache(cacheKey, sheetName) {

    var _array = [];
    var _string = '';
    var _stringCaches = []; // an array of JSON strings capped at length

    SpreadsheetApp.setActiveSpreadsheet(SpreadsheetApp.openById(CentralData()));
    var CD = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = CD.setActiveSheet(CD.getSheetByName(sheetName));
    _array = sheet.getRange(1, //starting row index
        1, // starting column index
        sheet.getLastRow(), // number of rows to import
        sheet.getLastColumn() // number of columns to import
        ).getValues(); // an array of rows, each an array of columns wholeList[r-1][c-1]

    // use the JSON.stringify method to convert the 2d array into a string for cache storage
    _string = JSON.stringify(_array);

    // check the JSON string length and break it up into smaller strings
    // !*!*!*!*!*!
    /* ! */ var MSL = 100000; // max string length
    // !*!*!*!*!*!
    for (; _string.length > 0;) {
        if (_string.length >= MSL) { // while the string is greater than or equal to the max value
            _stringCaches.push(_string.substr(0, MSL)); // split out the first MSL (e.g. 100,000) charaters
            _string = _string.substr(MSL); // write the rest of the string back
        }
        if (_string.length < MSL && _string.length > 0) {
            _stringCaches.push(_string); // write the whole remaining string
            _string = ''; // then delete the string
        }
    } // end string splitting loop

    // store the number of sub-caches in the "parent" cache
    CacheService.getDocumentCache().put(cacheKey, _stringCaches.length.toString());

    // use a loop to write the split caches
    for (; _stringCaches.length > 0;) {
        CacheService.getDocumentCache().put((cacheKey + _stringCaches.length), _stringCaches.pop());
    }

    return _array;
} // end fn:putCache

function getCache(cacheKey) {
    var cache = CacheService.getDocumentCache();
    var noOfCaches: number = parseInt(cache.get(cacheKey));
    var _array = [];
    var _string = '';

    if (noOfCaches != null) {

        // write the string values of the returned splpit cache into one string
        for (var i = 1; i <= noOfCaches; i++) {
            _string += (cache.get(cacheKey + i));
        }
        _array = JSON.parse(_string); // use the JSON.parse method to convert the cached string back into a 2d array
    } // if noOfCaches is not null

    return _array;

} // end fn:getCache

function getCL() {
    var _array = [];
    var _string = '';

    // check the "out of function" placeholder to speed up multiple calls to the cache
    if (CL) { return CL; }

    var cache = CacheService.getDocumentCache();
    _string = cache.get(CD_CoreList);
    if (_string != null) {
        _array = getCache(CD_CoreList);
        // set the "out of function" placeholder to speed up multiple calls to the cache
        CL = _array;
        return CL;
    }

    // store current spreadsheet settings
    var origSS = SpreadsheetApp.getActiveSpreadsheet();
    var origSH = origSS.getActiveSheet();
    var origRange = origSH.getActiveRange();

    _array = putCache(CD_CoreList, CD_CoreList);

    // return to the original spreadsheet
    SpreadsheetApp.setActiveSpreadsheet(origSS);
    SpreadsheetApp.setActiveSheet(origSH).setActiveRange(origRange);

    return _array;

} // end fn:getCL

function getWTSup() {

    var _array = [];
    var _string = '';

    var cache = CacheService.getDocumentCache();
    _string = cache.get(CD_WTSuppliers);
    if (_string != null) {
        _array = getCache(CD_WTSuppliers);
        return _array;
    }

    // store current spreadsheet settings
    var origSS = SpreadsheetApp.getActiveSpreadsheet();
    var origSH = origSS.getActiveSheet();
    var origRange = origSH.getActiveRange();

    _array = putCache(CD_WTSuppliers, CD_WTSuppliers);

    // return to the original spreadsheet
    SpreadsheetApp.setActiveSpreadsheet(origSS);
    SpreadsheetApp.setActiveSheet(origSH).setActiveRange(origRange);

    return _array;

} // end fn:getWTSup

function getCentralDropDowns() {

    var _object = {};
    var _string = '';
    var _CDdd = 'CDdd'; // the key name for the central data dropdowns


    var cache = CacheService.getDocumentCache();
    _string = cache.get(_CDdd);
    if (_string != null) {
        _object = JSON.parse(_string); // use the JSON.parse method to convert the cached string back an object of arrays
        return _object;
    }

    // store current spreadsheet settings
    var origSS = SpreadsheetApp.getActiveSpreadsheet();
    var origSH = origSS.getActiveSheet();
    var origRange = origSH.getActiveRange();

    SpreadsheetApp.setActiveSpreadsheet(SpreadsheetApp.openById(CentralData()));
    var CD: GoogleAppsScript.Spreadsheet.Spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    // the values in "" relate to the range names in the central data sheet
    // .join().split(',') is used to convert the 2d arrays into 1d arrays of string
    var TEMPLATE_IDS = CD.getRangeByName("TEMPLATE_IDS").getValues();
    var DRIVE_OWNER = CD.getRangeByName("DRIVE_OWNER").getValues();
    var ddType = CD.getRangeByName("dd_Type").getValues().join().split(',');
    var ddTeams = CD.getRangeByName("dd_Teams").getValues().join().split(',');
    var ddUoM = CD.getRangeByName("dd_UoM").getValues().join().split(',');
    var ddPDN = CD.getRangeByName("PD_Names").getValues().join().split(',');
    var ddPDC = CD.getRangeByName("PD_Codes").getValues().join().split(',');
    var ddVATRates = CD.getRangeByName("dd_VATRates").getValues().join().split(',');
    var ddWTSupNames = CD.getRangeByName("dd_WTSupNames").getValues().join().split(',');
    var ddStatusWT50 = CD.getRangeByName("dd_StatusWT50").getValues().join().split(',');
    var ddStatusBPR = CD.getRangeByName("dd_StatusBPR").getValues().join().split(',');
    var ddStatusHire = CD.getRangeByName("dd_StatusHire").getValues().join().split(',');

    _object = {
        DRIVE_OWNER: DRIVE_OWNER, TEMPLATE_IDS: TEMPLATE_IDS
        , ddType: ddType, ddTeams: ddTeams
        , ddUoM: ddUoM, ddPDN: ddPDN, ddPDC: ddPDC
        , ddVATRates: ddVATRates, ddWTSupNames: ddWTSupNames
        , ddStatusWT50: ddStatusWT50, ddStatusHire: ddStatusHire, ddStatusBPR: ddStatusBPR
    };

    // use the JSON.stringify method to convert the 2d array into a string for cache storage
    _string = JSON.stringify(_object);

    // store the array in the cache
    CacheService.getDocumentCache().put(_CDdd, _string);

    // return to the original spreadsheet
    SpreadsheetApp.setActiveSpreadsheet(origSS);
    SpreadsheetApp.setActiveSheet(origSH).setActiveRange(origRange);

    return _object;

} // end fn:getDropDowns

/*
 * End functions originally in CD Cache.gs
 */

/*
 * Functions originally in CoreList.gs 
 */

function setupCLPicker() {

    getCL();
    var CDD = getCentralDropDowns();
    /*  setCLDropDown(CL,ddPackage,clPackage);
    */


    CDD.ddPDN.pop(); // remove the last (void) entry

    var cells = [ // an array of columnHeader and dropDown datasource pairs
        ddPDN, CDD.ddPDN
        , ddTeamRequested, CDD.ddTeams
    ];

    for (; cells.length > 0;) {
        var cell = cells.shift(); // the cell reference (first of the array pair)
        var dv = SpreadsheetApp.newDataValidation();
        dv.setAllowInvalid(false);
        var _dd = cells.shift(); // the dropdown source datalist, (second of the array pair)
        dv.requireValueInList(_dd, true);
        SS.getSheetByName(CORELIST_SHEET).getRange(cell).setDataValidation(dv.build());
    } // end for loop

}

function clEdit(e) {
    var eRangeA1 = e.range.getA1Notation();
    var eValues = e.range.getValues();

    if ((eRangeA1 == ddPDN) || (eRangeA1 == ddTrade) || (eRangeA1 == ddSubcategory) || (eRangeA1 == ddType) || (eRangeA1 == ddViewAll)) {
        SS.toast("Please wait...", "Working", 600);
        setCLFilters(e);
    }

    if (eRangeA1 == ddSendBasket && e.value == 'GO!') {
        SS.toast("Please wait...", "Working", 600);
        sendBasket(e);
    }

    if (e.range.getRow() == clSortRow) {

        var sortDir = null;

        if (eValues[0][0] == 'A > Z') { sortDir = true; }
        if (eValues[0][0] == 'Z > A') { sortDir = false; }

        if (sortDir != null) {
            SH.getRange((clHeaderRow + 1), //starting row index
                1, // starting column index
                SH.getLastRow(), // number of rows to import
                SH.getLastColumn() // number of columns to import
                ).sort({ column: e.range.getColumn(), ascending: sortDir });
        }// end if sort is valid
        e.range.setValue('Sort?');
    } // end if sort row was edited

    if (e.range.getRow() > clHeaderRow) {

        if (e.range.getColumn() == 7) { // check if the quantity column was edited
            var _PDN = SH.getRange(ddPDN).getValue();
            // the budget range is the same size as the edited range, but 2 columns to the right
            var budgetPDNRange = e.range.offset(0, 4);
            var budgetPDNValues = budgetPDNRange.getValues();

            for (var i = 0; i < eValues.length; i++) {
                if (eValues[i][0] > 0 && budgetPDNValues[i][0] == '') {
                    budgetPDNValues[i][0] = _PDN;
                } // end if a quantity was picked and the item didn't already have a pdn
                if (!eValues[i][0]) {
                    budgetPDNValues[i][0] = '';
                } // end if the quanity was set to 0 and so the pdn was cleared
            } // end loop through edited quantity values vs budget pdn

            budgetPDNRange.setValues(budgetPDNValues);

        } // end if quantity column was edited

        // grab everything, including the basket header
        var currentFLRange = SH.getRange(clHeaderRow, //starting row index
            1, // starting column index
            SH.getLastRow(), // number of rows to import
            SH.getLastColumn() // number of columns to import
            );

        var basketReturn = basketList(currentFLRange.getValues());

        var basket = basketReturn.basket; // an array for the whole basket (every item with qty>0)
        var basketICs = basketReturn.ics; // an array of just the item codes (as strings) of the items in the basket
        var basketDLs = basketReturn.dls; // an array of just the item codes data links (as strings) of the items in the basket

        var duplicates = checkForDuplicates(basketICs);

        if (duplicates) {
            SH.getRange(dupWarningCell).setValue('Duplicates\nFound');
            SH.getRange(dupResultsCell).setValue(duplicates.join('\n'));
        } // end if duplicates are found
    } // end if basket area was edited

} // end fn:clEdit

// e is the event object from the edit event in Code.gs
function setCLFilters(e) {

    var cellRef = e.range.getA1Notation();
    var cellValue = e.value;
    if (!cellValue) { cellValue = ''; }

    var fl = []; // the filtered core list

    /*  var packageVal = SH.getRange(ddPackage).getValue();
      if (!packageVal){packageVal='';}
      var flPackage = filterCLList(getCL(),packageVal,clPackage);
    */

    var viewAllVal = SH.getRange(ddViewAll).getValue();

    var PDCVal = PD(SH.getRange(ddPDN).getValue());
    if (!PDCVal) { PDCVal = ''; }
    var flPDC = filterCLList(getCL(), PDCVal, clPDC);
    /*  // if a package has been chosen, limit the PDC selection to those items from that package,
      // ...which will cascade to sub and type
      if (packageVal.length>0){flPDC = filterCLList(flPackage,PDNVal,clPDC);}
    */

    var tradeVal = SH.getRange(ddTrade).getValue();
    var flTrade = filterCLList(flPDC, tradeVal, clTrade);

    var subVal = SH.getRange(ddSubcategory).getValue();
    var flSub = filterCLList(flTrade, subVal, clSubcategory);

    var typeVal = SH.getRange(ddType).getValue();
    var flType = filterCLList(flSub, typeVal, clType);

    /*  // >>>> Package dropdown was edited ***************** WIP
      // check which other filters are in place
      if (cellRef == ddPackage){
        if (cellValue.length>0){fl = flPackage;Logger.log("package");} // end if cellValue was present
        if (PDNVal.length>0){fl = flPDC;Logger.log("pdc");} // unnecessary line of code, but there for logical consitency
        if (subVal.length>0){fl = flSub;Logger.log("sub");}
        if (typeVal.length>0){fl = flType;Logger.log("type");}
        setCLDropDown(flPDC,ddPDN,clPDC);
        setCLDropDown(flSub,ddSubcategory,clSubcategory);
        setCLDropDown(flType,ddType,clType);
      } // end if package
    */
    // >>>> PDC dropdown was edited
    if (cellRef == ddPDN) {
        clearCells([ddTrade, ddSubcategory, ddType]);
        if (cellValue.length > 0) {
            fl = flPDC;
            setCLDropDown(fl, ddTrade, clTrade);
        } // end if cellValue was present
    } // end if PDC

    // >>>> Trade dropdown was edited
    if (cellRef == ddTrade) {
        clearCells([ddSubcategory, ddType]);
        if (cellValue.length > 0) {
            fl = flTrade;
            setCLDropDown(fl, ddSubcategory, clSubcategory);
        } // end if cellValue was present
    } // end if Trade

    // >>>> Subcategory dropdown was edited
    if (cellRef == ddSubcategory) {
        clearCells([ddType]);
        if (cellValue.length > 0) {
            fl = flSub;
            setCLDropDown(fl, ddType, clType);
        } // end if cellValue was present
        if (cellValue.length == 0) {
            fl = flTrade;
            //      setCLDropDown(fl,ddType,clType);
        } // reset to flPDC level of filter if subcategory was deleted
    } // end if subcategory

    // >>>> Type dropdown was edited
    if (cellRef == ddType) {
        if (cellValue.length > 0) {
            fl = flType;
        } // end if cellValue was present
        if (cellValue.length == 0) {
            fl = flSub;
        } // reset to flSub level of filter if type was deleted
    } // end if type

    // >>>> ViewAll dropdown was edited
    if (cellRef == ddViewAll) {
        if (cellValue == 'View All') {
            clearCells([ddPDN, ddTrade, ddSubcategory, ddType]);
            fl = getCL();
        } // end if cellValue was to view all
        if (cellValue != 'View All') {
            setupCLPicker();
        }
    } // end if ViewAll

    // filter the CL
    displayFilteredCLList(fl);
    SS.toast("Thanks for waiting", "Done", 1);
} // fn:setCLFilters

function displayFilteredCLList(fl) {

    // grab everything, including the basket header
    var currentFLRange = SH.getRange(clHeaderRow, //starting row index
        1, // starting column index
        SH.getLastRow(), // number of rows to import
        SH.getLastColumn() // number of columns to import
        );

    var basketReturn = basketList(currentFLRange.getValues());

    var basket = basketReturn.basket; // an array for the whole basket (every item with qty>0)
    var basketICs = basketReturn.ics; // an array of just the item codes (as strings) of the items in the basket
    var basketDLs = basketReturn.dls; // an array of just the item codes data links (as strings) of the items in the basket
    var basketPDNs = basketReturn.pdns; // an array of just the project dimension names (as strings) of the items in the basket

    var duplicates = checkForDuplicates(basketICs);

    if (duplicates) {
        SH.getRange(dupWarningCell).setValue('Duplicates\nFound');
        SH.getRange(dupResultsCell).setValue(duplicates.join('\n'));
    }


    // clear the basket area, except for the header  
    SH.getRange((clHeaderRow + 1), //starting row index
        1, // starting column index
        SH.getLastRow(), // number of rows to import
        SH.getLastColumn() // number of columns to import
        ).clearContent();

    // dl is the display list. This is fl with filter columns (trade, sub, type, etc.) removed
    var dl = [];
    var dlDataLinks = [];

    var _PDN = SH.getRange(ddPDN).getValue();

    // start var row at 1 to ignore header row of fl
    for (var row = 1; row < fl.length; row++) {

        var _IC = fl[row][fl[0].indexOf(clIC)];
        if (basketICs.indexOf(_IC) < 0 // check that the item code for this display item isn't in the basket
        // or if it is, that it is from a PDN not already used
            || (basketICs.indexOf(_IC) >= 0 && basketPDNs.indexOf(_PDN) < 0)
            ) {
            var dlItem = [];
            var dlDataLink = [];
            // push in the order needed for the basket columns 
            dlItem.push(_IC);
            dlItem.push(fl[row][fl[0].indexOf(clID)]);
            dlItem.push(fl[row][fl[0].indexOf(clMfg)]);
            dlItem.push(fl[row][fl[0].indexOf(clBrand)]);
            dlItem.push(fl[row][fl[0].indexOf(clPartNo)]);
            dlItem.push(fl[row][fl[0].indexOf(clLastPrice)]);
            dlItem.push(''); // a blank entry for the quantity requested column
            dlItem.push(fl[row][fl[0].indexOf(clPUoM)]);
            dlItem.push(fl[row][fl[0].indexOf(clFactor)]);
            dlItem.push(fl[row][fl[0].indexOf(clBUoM)]);

            dl.push(dlItem);

            // get the data link, if available
            var _dataLink = fl[row][fl[0].indexOf(clDataLink)];
            dlDataLink.push('=HYPERLINK(\"' + _dataLink + '\",\"' + _IC + '\")');
            dlDataLinks.push(dlDataLink);
        } // end if dl item is not already in the basket
    } // end for loop

    basket.shift(); // remove the header row

    if (basket.length > 0) {
        // get a range equal in row length to the basket. Write even if there is no basket so that the header is written back in.
        SH.getRange((clHeaderRow + 1), //starting row index
            1, // starting column index
            basket.length, // number of rows to import
            basket[0].length // number of columns to import 
            ).setValues(basket); // set this range with the contents of the filtered list.

        SH.getRange((clHeaderRow + 1), //starting row index, just below header row
            1, // starting column index
            basketDLs.length, // number of rows to import
            1 // number of columns to import 
            ).setFormulas(basketDLs); // set this range with the formula contents of the filtered list.    
    }

    // only export the filtered display list if one exists
    if (dl.length > 0) {
        // get a range equal in row length to the filtered list and start below the existing basket
        SH.getRange((clHeaderRow + 1 + basket.length), //starting row index
            1, // starting column index            
            dl.length, // number of rows to import
            dl[0].length // number of columns to import                
            ).setValues(dl); // set this range with the contents of the filtered list.

        // write the data links into the first column
        SH.getRange((clHeaderRow + 1 + basket.length), //starting row index
            1, // starting column index            
            dlDataLinks.length, // number of rows to import
            1 // number of columns to import                
            ).setFormulas(dlDataLinks); // set this range with the formula contents of the filtered list.    

    } // end if display list has content / exists

    // auto resize all the basket columns
    //for (var c=1;c<9;c++){SH.autoResizeColumn(c);}
    SH.getRange(1, 1, 1000, 1).setNumberFormat('@STRING@'); // set the first column to plain text
    //  SH.autoResizeColumn(2); 

} // end fn:displayFilteredList

function checkForDuplicates(basketICs) {

    SH.getRange(dupWarningCell + ':' + dupResultsCell).clearContent();

    // check for duplicated items already on the materials list
    var tempMatSh = SS.getSheetByName(MATERIALS_SHEET);
    var tempMatList = tempMatSh.getRange(1, 1, tempMatSh.getLastRow(), 9).getValues();

    var duplicatesArray = [];

    for (var t = (tempMatList.length - 1); t > 0; t--) {
        // if this iteration of the materials list has an item code that matches anything in the basket...
        var thisIC = tempMatList[t][tempMatList[MHRI].indexOf(H_ICODE)];
        var thisICasString = thisIC.toString();
        if (basketICs.indexOf(thisIC) > -1 || basketICs.indexOf(thisICasString) > -1) {
            //... insert tab separated row number, line id, item code and existing quantity into the duplicates array
            var dA = [
                (t + 1),
                (tempMatList[t][tempMatList[MHRI].indexOf(H_LINEID)]),
                thisIC,
                (tempMatList[t][tempMatList[MHRI].indexOf(H_QTY)])
            ];
            duplicatesArray.unshift(dA.join('\t'));
        } // end if duplicate has been found
    } // end for loop 

    // if duplicates were found, insert a header "row"
    if (duplicatesArray.length > 0) {
        var _dA = ['Row', 'Line', 'Item', 'Qty'];
        duplicatesArray.unshift(_dA.join('\t'));
        return duplicatesArray;
    }

    return null;

} /// end fn:checkForDuplicates

function filterCLList(list, ddValue, ddOption) {
    var subList = [];
    // for the provided List, find all items for this dropDown option
    for (var i = 1; i < list.length; i++) {
        var li = list[i][list[0].indexOf(ddOption)].toString();//store the list item as a string
        if (li.indexOf(ddValue) > -1) { //check if this item's option field contains a string match to the search value
            subList.push(list[i]);
        } // end if
    } // end for loop
    subList.unshift(getCL()[0]);// add in the title row at the beginning
    return subList;
} // end fn:filterList

function setCLDropDown(list, ddCell_A1, ddType) {
    var aOpt = []; // an array of the dropdown options
    // for the provided List, find all available options for this Type. Start at i=1 to remove header
    // the header is unshifted back to the top of the array at the end of fn:filterList
    for (var i = 1; i < list.length; i++) {
        var opt = list[i][list[0].indexOf(ddType)];
        if (aOpt.indexOf(opt) < 0) {
            aOpt.push(opt); // if this type isn't already in the dropdown option list, add it
        } // end if
    } // end for loop
    aOpt.sort(); // sort alphabetically

    var dv = SpreadsheetApp.newDataValidation();
    dv.setAllowInvalid(false);
    dv.requireValueInList(aOpt, true);
    SS.getSheetByName(CORELIST_SHEET).getRange(ddCell_A1).setDataValidation(dv.build());

} // end fn:setCLDropDown

function basketList(list) {
    var subList = [];
    var icList = [];
    var dlList = [];
    var pdnList = [];
    // for the provided List, find all items with a quantity greater than one
    for (var i = 1; i < list.length; i++) {
        if (list[i][list[0].indexOf(bQty)] > 0) {
            subList.push(list[i]); // the whole item
            pdnList.push(list[i][list[0].indexOf(bPDN)]);
            var _ic = list[i][list[0].indexOf(bIC)].toString();
            icList.push(_ic); // the item code as a string
            var _dataLink = searchCLbyIC(_ic, clDataLink);
            var _dlFormula = [];
            _dlFormula.push('=HYPERLINK(\"' + _dataLink + '\",\"' + _ic + '\")');
            dlList.push(_dlFormula);
        } // end if
    } // end for loop
    subList.unshift(list[0]);// add in the title row at the beginning
    var retVal = { basket: subList, ics: icList, dls: dlList, pdns: pdnList };
    return retVal;
} // end function

function sendBasket(e) {

    // store the last column index value so that it's not lost after range.clear()
    var clLastCol = SH.getLastColumn();

    // grab everything, including the basket header
    var currentFLRange = SH.getRange(clHeaderRow, //starting row index
        1, // starting column index
        SH.getLastRow(), // number of rows to import
        clLastCol // number of columns to import
        );

    var basketReturn = basketList(currentFLRange.getValues());

    var basket = basketReturn.basket; // an array for the whole basket (every item with qty>0)
    var basketICs = basketReturn.ics; // an array of just the item codes (as strings) of the items in the basket
    var basketDLs = basketReturn.dls; // an array of just the item codes data links (as strings) of the items in the basket

    var duplicates = checkForDuplicates(basketICs);

    Logger.log('tested for duplicates');

    /*  if (duplicates){
    
        Logger.log('duplicates found');
    
        var duplicatesAlert = UI.alert("Item Duplication",
                                       "Item(s) in your Core List Basket are already present on the Materials List."
                                       +"\nA summary of any duplication is shown at the top left of the Core List Picker."
                                       +"\nChoose \"OK\" to continue and add your basket to the Materials List, or \"Cancel\" to stop and resolve the duplication.",
                                       UI.ButtonSet.OK_CANCEL);

        if (duplicatesAlert == UI.Button.OK){
          Logger.log('said yes to process duplicates');
        }

        if (duplicatesAlert == UI.Button.CANCEL){

          SH.getRange(dupWarningCell).setValue('Duplicates\nFound');  
          SH.getRange(dupResultsCell).setValue(duplicates.join('\n'));

          e.range.setValue("Add to Materials Tracker");
          SS.toast(".", ".", 1);
          return;
        }

      } // end if duplicates were found
    */
    Logger.log('basket size is: ' + (basket.length - 1));

    if (basket.length == 1 && e.value == 'GO!') {
        e.range.setValue("Add to Materials Tracker");
        SS.toast(".", ".", 1);
        UI.alert("No Items to Add", "There were no basket items found to add to the materials tracker", UI.ButtonSet.OK);
        return;
    }

    currentFLRange.clearContent(); // clear the filter view

    var basketHeader = SH.getRange(clHeaderRow, 1, 1, clLastCol);
    basketHeader.setValues([basket[0]]);

    _sh = SS.getSheetByName(MATERIALS_SHEET)
  var curMatRange = _sh.getRange(1, //starting row index
        2, // starting column index
        _sh.getLastRow(), // number of rows to import
        _sh.getLastColumn() // number of columns to import 
        );

    var curMat = curMatRange.getValues();
    var blankRow = 0;

    for (var i = curMat.length - 1; i > MHRI; i--) {
        // check for the first blank row
        if (
            (curMat[i][curMat[MHRI].indexOf(H_IDESC)] != '')
            || (curMat[i][curMat[MHRI].indexOf(H_ICODE)] != '')
            || (curMat[i][curMat[MHRI].indexOf(H_TYPE)] != '')
            ) { blankRow = i + 2; break; }
    } // end for loop

    // basketExport (be) is the range [r][c] of ordered columns for the materials list
    var be = [];
    var bePDN = [];
    var beSupplier = [];
    var beStatus = [];
    // start var r at 1 to ignore header row of fl
    for (var b = 1; b < basket.length; b++) {
        var beItem = [];
        var clType = CORELIST;
        // push in the order needed for the basket columns to match the materials tracking sheet
        beItem.push(basket[b][basket[0].indexOf(bID)]);
        beItem.push(basket[b][basket[0].indexOf(bIC)]);
        if (beItem[1].indexOf('(') > -1) { clType = COREEXTRA; } // parenthesis in the item code indicates Free Text item which is a Core Extra
        beItem.push(clType);
        beItem.push(''); // a blank entry for the notes column
        beItem.push(basket[b][basket[0].indexOf(bUsage)]);
        beItem.push(SH.getRange(ddTeamRequested).getValue());
        beItem.push(basket[b][basket[0].indexOf(bQty)]);
        beItem.push(basket[b][basket[0].indexOf(bPUoM)]);
        beItem.push(basket[b][basket[0].indexOf(bFactor)]);
        beItem.push(basket[b][basket[0].indexOf(bBUoM)]);
        beItem.push(basket[b][basket[0].indexOf(bLastPrice)]);
        be.push(beItem);

        var _bePDN = [];
        _bePDN.push(basket[b][basket[0].indexOf(bPDN)]);
        bePDN.push(_bePDN)

    var _beSupplier = [];
        _beSupplier.push(PR_SUPPLIER)
    beSupplier.push(_beSupplier);

        var _beStatus = [];
        _beStatus.push('1 To Be Reviewed');
        beStatus.push(_beStatus);

    } // end for loop

    _sh.getRange(blankRow, //starting row index
        col(H_IDESC).n, // starting column index
        be.length, // number of rows to import
        be[0].length // number of columns to import 
        ).setValues(be);

    _sh.getRange(blankRow, //starting row index
        col(H_PDN).n, // starting column index
        bePDN.length, // number of rows to import
        bePDN[0].length // number of columns to import 
        ).setValues(bePDN);

    _sh.getRange(blankRow, //starting row index
        col(H_SUPPLIER).n, // starting column index
        beSupplier.length, // number of rows to import
        beSupplier[0].length // number of columns to import 
        ).setValues(beSupplier);

    _sh.getRange(blankRow, //starting row index
        col(H_STATUS).n, // starting column index
        beStatus.length, // number of rows to import
        beStatus[0].length // number of columns to import 
        ).setValues(beStatus);

    // set the "completed" notification, clear the filter dropdowns and reset them.
    e.range.setValue("Add to Materials Tracker");
    clearCells([ddPDN, ddTrade, ddSubcategory, ddType]);
    setupCLPicker();
    SS.toast("Thanks for waiting", "Done", 1);
}

function searchCLbyIC(IC, searchHeader) {
    CL = getCL();
    var retVal = null;
    for (var i = 0; i < CL.length; i++) {
        // search through for the IC
        if (CL[i][CL[0].indexOf(clIC)] == IC) {
            retVal = CL[i][CL[0].indexOf(searchHeader)];
            break;
        } // end if ic found
    } // end for loop
    return retVal;
} // end fn:searchCLbyIC

function clearCells(cellsA1) {
    for (; cellsA1.length > 0;) {
        SH.getRange(cellsA1.pop()).clearContent().clearDataValidations();
    }
} // end fn:clearCells

/*
 * End functions originally in CoreList.gs
 */

/*
 * Functions originally in GoodsReceiving.gs

 */
/*
* some functionality is shared with the core list 
* particularly dropdown filtering and display.
* however, to maintain independance, functions are duplicated
*/

function setupGoodsReceiver() {
    /*
    var CDD = getCentralDropDowns();
    
    var cells = [ // an array of columnHeader and dropDown datasource pairs
      ddPurchasingRoute , [grSHOWALLopt,grCOREopt,grWTopt]
    ];
  
    for (;cells.length>0;){
      var cell = cells.shift(); // the cell reference (first of the array pair)
      var dv = SpreadsheetApp.newDataValidation();
      dv.setAllowInvalid(false);
      var _dd = cells.shift(); // the dropdown source datalist, (second of the array pair)
      dv.requireValueInList(_dd, true);
      SS.getSheetByName(GOODS_RECEIVING_SHEET).getRange(cell).setDataValidation(dv.build());
    } // end for loop
  */
}

function getML() {
    if (!MATLIST) {
        MATLIST = matSH.getRange(
            (MHRI + 1), 1, matSH.getLastRow(), matSH.getLastColumn()
            ).getValues();
    }
    return MATLIST;
} // end fn:getML

function grEdit(e) {
    var eRangeA1 = e.range.getA1Notation();
    var eValues = e.range.getValues();

    if (eRangeA1 == ddPurchasingRoute && e.value == 'GO!') {
        SS.toast("Please wait...", "Working", 600);
        setGRFilters();
    }

    if (eRangeA1 == ddMarkReceived && e.value == 'GO!') {
        SS.toast("Please wait...", "Working", 600);
        markGoodsReceived();
    }

    if (e.range.getRow() == grSortRow) {

        var sortDir = null;

        if (eValues[0][0] == 'A > Z') { sortDir = true; }
        if (eValues[0][0] == 'Z > A') { sortDir = false; }

        if (sortDir != null) {
            SH.getRange((clHeaderRow + 1), //starting row index
                1, // starting column index
                SH.getLastRow(), // number of rows to import
                SH.getLastColumn() // number of columns to import
                ).sort({ column: e.range.getColumn(), ascending: sortDir });
        }// end if sort is valid
        e.range.setValue('Sort?');
    } // end if sort row was edited

    if (e.range.getRow() > grHeaderRow) {

        if (e.range.getColumn() == 12) { // check if the quantity column was edited
            var thisDate = new Date();
            // the budget range is the same size as the edited range, but 2 columns to the right
            var dateRange = e.range.offset(0, 1);
            var dateValues = dateRange.getValues();

            for (var i = 0; i < eValues.length; i++) {
                if (eValues[i][0] > 0 && dateValues[i][0] == '') {
                    dateValues[i][0] = thisDate;
                } // end if a quantity was picked and the item didn't already have a pdn
                if (!eValues[i][0]) {
                    dateValues[i][0] = '';
                } // end if the quanity was set to 0 and so the pdn was cleared
            } // end loop through edited quantity values vs budget pdn

            dateRange.setValues(dateValues);

        } // end if quantity column was edited

    } // end if basket area was edited

} // end fn:grEdit

// e is the event object from the edit event in Code.gs
function setGRFilters() {

    var fl = []; // the filtered list

    var purchasingRouteVal = SH.getRange(ddPurchasingRoute).getValue();
    if (!purchasingRouteVal) { purchasingRouteVal = ''; }
    var flPurchasingRoute = filterGRList(getML(), purchasingRouteVal, H_TYPE);
    if (purchasingRouteVal == grSHOWALLopt) {
        var flCore = filterGRList(getML(), grCOREopt, H_TYPE);
        var flWT = filterGRList(getML(), grWTopt, H_TYPE);
        flWT.shift(); // remove the first entry which is the header
        fl = flCore.concat(flWT);
    }

    Logger.log(flPurchasingRoute);

    /* 
      var supplierVal = SH.getRange(ddSupplier).getValue();
      var supplierColHeader = H_BRANCHSUPPLIER;  
      if (purchasingRouteVal == grWTopt){supplierColHeader = H_SUPPLIER;}
      if (!supplierVal){supplierVal = '';}
      var flSupplier = filterGRList(flPurchasingRoute,supplierVal,supplierColHeader);  
  
      // date . . . needs work
    //  var typeVal = SH.getRange(ddType).getValue();
    //  var flType = filterList(flSub,typeVal,clType);
  
      // >>>> Purchasing Route dropdown was edited
      if (cellRef == ddPurchasingRoute){
        clearCells([ddSupplier]);
        SH.showColumns(supplierColNum);
        if (cellValue.length>0){
          fl = flPurchasingRoute;
          setGRDropDown(fl,ddSupplier,supplierColHeader);
        } // end if cellValue was present
        if (cellValue == grWTopt){
          SH.hideColumns(mfgColNum);
          SH.hideColumns(partColNum);
        } 
        if (cellValue == grCOREopt || cellValue == grSHOWALLopt){
          SH.showColumns(mfgColNum);
          SH.showColumns(partColNum);
        }
      } // end if Purchasing Route
  
      // >>>> Supplier dropdown was edited
      if (cellRef == ddSupplier){
        if (cellValue.length>0){
          fl = flSupplier;
          SH.hideColumns(supplierColNum);
        } // end if cellValue was present
        if (cellValue.length==0){
          fl=flPurchasingRoute;
          SH.showColumns(supplierColNum);
        } // reset to flPurchasingRoute level of filter if subcategory was deleted
      } // end if Supplier
    */
    // filter the GR List
    clearDisplayGRList(); // clear the currently displayed list

    var isCore = true;
    //  if (purchasingRouteVal == grWTopt){isCore=false;}
    //  if (purchasingRouteVal != grSHOWALLopt){compileFilteredGRList(fl,isCore);}

    compileFilteredGRList(fl, true);

    SH.getRange(grLastUpdated).setValue(new Date());
    SH.getRange(ddPurchasingRoute).setValue('Refresh Items');

    SH.getRange((clHeaderRow + 1), //starting row index
        1, // starting column index
        SH.getLastRow(), // number of rows to import
        SH.getLastColumn() // number of columns to import
        ).sort({ column: 8, ascending: true });

    SS.toast("Thanks for waiting", "Done", 1);

} // fn:setGRFilters

function clearDisplayGRList() {
    // grab everything below the header
    var currentFLRange = SH.getRange((grHeaderRow + 1), //starting row index
        1, // starting column index
        SH.getLastRow(), // number of rows to import
        SH.getLastColumn() // number of columns to import 
        );

    var currentFLValues = currentFLRange.getValues();

    // check if anything has been marked goods received

    currentFLRange.clearContent(); // clear the filter view
} // end fn:clearDisplayGRList

function compileFilteredGRList(fl, isCore) {

    // dl is the display list. This is fl with filter columns removed
    var dl = [];
    var dlDataLinks = [];

    // start var row at 1 to ignore header row of fl
    for (var row = 1; row < fl.length; row++) {
        var _IC = fl[row][fl[0].indexOf(H_ICODE)];
        var dlItem = [];
        var dlDataLink = [];

        // push in the order needed for the display columns 
        dlItem.push(fl[row][fl[0].indexOf(H_LINEID)]); // col 1
        dlItem.push(_IC); // col 2
        dlItem.push(fl[row][fl[0].indexOf(H_IDESC)]); // col 3

        if (isCore) {
            dlItem.push(searchCLbyIC(_IC, clMfg)); // col 4
            dlItem.push(searchCLbyIC(_IC, clPartNo)); // col 5
            dlItem.push(fl[row][fl[0].indexOf(H_BRANCHSUPPLIER)]); // col 6
            dlItem.push(
                'PR: ' + fl[row][fl[0].indexOf(H_PONUM)]
                + ' /PO: ' + fl[row][fl[0].indexOf(H_BRANCHPO)]
                ); // col 7

            // get the data link, if available
            var _dataLink = searchCLbyIC(_IC, clDataLink);
            dlDataLink.push('=HYPERLINK(\"' + _dataLink + '\",\"' + _IC + '\")');
            dlDataLinks.push(dlDataLink);

        } // end if isCore

        if (!isCore) {
            dlItem.push(''); // col 4, no manufacturer for WT50
            dlItem.push(''); // col 5, no part number for WT50
            dlItem.push(fl[row][fl[0].indexOf(H_SUPPLIER)]); // col 6
            dlItem.push(WT_PREFIX + ' ' + PROJ_NUMBER() + ' ' + fl[row][fl[0].indexOf(H_PONUM)]); // col 7
        } // end is not Core

        dlItem.push(fl[row][fl[0].indexOf(H_ACTDEL)]); // col 8
        dlItem.push(fl[row][fl[0].indexOf(H_QTY)]); // col 9
        dlItem.push(fl[row][fl[0].indexOf(H_QTYLEFT)]); // col 10
        dlItem.push(fl[row][fl[0].indexOf(H_PUOM)]); // col 11

        dl.push(dlItem);

    } // end for loop

    if (dl.length > 0) {
        // get a range equal in row length to the filtered list and write anyway to reinsert the header
        SH.getRange((grHeaderRow + 1), //starting row index
            1, // starting column index            
            dl.length, // number of rows to import
            dl[0].length // number of columns to import                
            ).setValues(dl); // set this range with the contents of the filtered list.

        if (isCore) {
            // write the data links into the first column, if the purchasing route isCore
            SH.getRange((grHeaderRow + 1), //starting row index
                2, // starting column index            
                dlDataLinks.length, // number of rows to import
                1 // number of columns to import                
                ).setFormulas(dlDataLinks); // set this range with the formula contents of the filtered list.
        } // end if isCore    
    } // end if display lisy exists

    // auto resize selected columns
    /*  SH.autoResizeColumn(3);
      if (isCore){
        SH.autoResizeColumn(4);
        SH.autoResizeColumn(5);
      }
      SH.autoResizeColumn(6);
    */

} // end fn:getFilteredGRList


function filterGRList(list, ddValue, ddOption) {
    var subList = [];
    // for the provided List, find all items for this dropDown option
    for (var i = (1); i < list.length; i++) {
        // check that the status is "pending delivery" and items are left to be delivered
        if (list[i][list[0].indexOf(H_STATUS)].toString().substr(0, 1) > 3 // status 2 is "ready to send"
            && list[i][list[0].indexOf(H_STATUS)].toString().substr(0, 1) < 7 // status 6 is "part delivered"
            && list[i][list[0].indexOf(H_QTYLEFT)] > 0) {

            var li = list[i][list[0].indexOf(ddOption)];//.toString();//store the list item as a string
            if (li.indexOf(ddValue) > -1) { //check if this item's option field contains a string match to the search value
                subList.push(list[i]);
            } // end if

        } // end if "pending delivery" and items left to deliver
    } // end for loop
    subList.unshift(getML()[0]);// add in the title row at the beginning
    return subList;
} // end fn:filterGRList

function setGRDropDown(list, ddCell_A1, ddType) {
    var aOpt = []; // an array of the dropdown options
    // for the provided List, find all available options for this Type. Start at i=1 to remove header
    // the header is unshifted back to the top of the array at the end of fn:filterList
    for (var i = 1; i < list.length; i++) {
        var opt = list[i][list[0].indexOf(ddType)];
        if (aOpt.indexOf(opt) < 0) {
            aOpt.push(opt); // if this type isn't already in the dropdown option list, add it
        } // end if
    } // end for loop
    aOpt.sort(); // sort alphabetically

    var dv = SpreadsheetApp.newDataValidation();
    dv.setAllowInvalid(false);
    dv.requireValueInList(aOpt, true);
    SS.getSheetByName(GOODS_RECEIVING_SHEET).getRange(ddCell_A1).setDataValidation(dv.build());

} // end fn:setGRDropDown

function markGoodsReceived() {

    SS.toast("Collecting Materials and Delivery Data", "Goods Receiving 1 of 3", 600);

    var grSH = SS.getSheetByName(GOODS_RECEIVING_SHEET);
    var grSHlr = grSH.getLastRow();

    // goods recieving columns

    var grLineIDRange = grSH.getRange((grHeaderRow + 1), 1, grSHlr, 1);
    var grLineIDVals = grLineIDRange.getValues();

    var grQTYRemRange = grSH.getRange((grHeaderRow + 1), 10, grSHlr, 1);
    var grQTYRemVals = grQTYRemRange.getValues();

    var grQTYRcvdRange = grSH.getRange((grHeaderRow + 1), 12, grSHlr, 1);
    var grQTYRcvdVals = grQTYRcvdRange.getValues();

    var grDateRcvdRange = grSH.getRange((grHeaderRow + 1), 13, grSHlr, 1);
    var grDateRcvdVals = grDateRcvdRange.getValues();

    var grNotesRange = grSH.getRange((grHeaderRow + 1), 14, grSHlr, 1);
    var grNotesVals = grNotesRange.getValues();

    // materials sheet columns

    var mtLineIDRange = matSH.getRange(matFirst, col(H_LINEID).n, matLast, 1);
    var mtLineIDVals = mtLineIDRange.getValues();
    // .join().split(',') is used to convert the 2d arrays into 1d arrays of string
    mtLineIDVals = mtLineIDVals.join().split(',');

    var mtQTYRcvdRange = matSH.getRange(matFirst, col(H_QTYRCVD).n, matLast, 1);
    var mtQTYRcvdVals = mtQTYRcvdRange.getValues();

    var mtDateRcvdRange = matSH.getRange(matFirst, col(H_DATEDELIVERED).n, matLast, 1);
    var mtDateRcvdVals = mtDateRcvdRange.getValues();

    var mtNotesRange = matSH.getRange(matFirst, col(H_DELCOMMENTS).n, matLast, 1);
    var mtNotesVals = mtNotesRange.getValues();

    var mtStatusRange = matSH.getRange(matFirst, col(H_STATUS).n, matLast, 1);
    var mtStatusVals = mtStatusRange.getValues();

    SS.toast("Matching Delivery Data", "Goods Receiving 2 of 3", 600);

    for (var g = 0; g < grQTYRcvdVals.length; g++) {

        if (grQTYRcvdVals[g][0] > 0) { //check if recevied qty is greater than 0

            // get the index number from the materials sheet for this lineID
            var mtIndex = mtLineIDVals.indexOf(grLineIDVals[g][0]);
            Logger.log(mtIndex);
            mtQTYRcvdVals[mtIndex][0] = grQTYRcvdVals[g][0];
            mtDateRcvdVals[mtIndex][0] = grDateRcvdVals[g][0];
            mtNotesVals[mtIndex][0] = grNotesVals[g][0];

            if (grQTYRcvdVals[g][0] < grQTYRemVals[g][0]) { mtStatusVals[mtIndex][0] = '6 Part Delivered'; }
            if (grQTYRcvdVals[g][0] >= grQTYRemVals[g][0]) { mtStatusVals[mtIndex][0] = '7 Delivered'; }

        } // end if qty>0 goods received

    } // end for loop

    SS.toast("Writing Delivery Data", "Goods Receiving 3 of 3", 600);

    mtQTYRcvdRange.setValues(mtQTYRcvdVals);
    mtDateRcvdRange.setValues(mtDateRcvdVals);
    mtNotesRange.setValues(mtNotesVals);
    mtStatusRange.setValues(mtStatusVals);

    SS.toast("Done", "Goods Received", 5);

    grSH.getRange(ddMarkReceived).setValue('Mark Goods Received');

    grSH.getRange(ddPurchasingRoute).setValue('GO!');
    setGRFilters();

} // end fn:markGoodsReceived

/*
 * End Functions originally in GoodsReceiving.gs
 */
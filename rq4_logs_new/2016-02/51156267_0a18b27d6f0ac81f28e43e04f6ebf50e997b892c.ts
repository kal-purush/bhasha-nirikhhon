module Materials {
    var matSH = SS.getSheetByName(MATERIALS_SHEET);
    var matFirst = MHRI + 2
var matLast = matSH.getLastRow();



    function matEdit(e) {

        /* write the supplier details into a note if the suppler dropdown was edited */
        if (e.range.getColumn() == col(H_SUPPLIER).n) {
            e.range.clearNote();
            matSH.getRange(col(H_ADMINCODE).a + e.range.getRow()).setValue('');
            var supplier = getSupplier(e.value);
            if (supplier.name == e.value) {
                var hoverNote = supplier.contact + '\n' + supplier.tel + '\n' + supplier.email;
                e.range.setNote(hoverNote);
                // set the WT Admin Code
                matSH.getRange(col(H_ADMINCODE).a + e.range.getRow()).setValue(supplier.admin);
            } // end if supplier has been found from lookup
        } // end if supplier column has been edited


        /* if the status has been set to a void value, add note to the PDN and PDC boxes then set PDN to void */
        if (e.range.getColumn() == col(H_STATUS).n) {
            var thisPDN = matSH.getRange(col(H_PDN).a + e.range.getRow());
            var thisPDC = matSH.getRange(col(H_PDC).a + e.range.getRow());

            if (e.value.substr(0, 1) >= VOID_PREFIX) {
                if (thisPDN.getNote() == '') {
                    thisPDN.setNote('was: ' + thisPDN.getValue());
                    thisPDC.setNote('was: ' + thisPDC.getValue());
                    thisPDN.setValue('void');
                }
            } // if the status was set to void

            if (e.value.substr(0, 1) < VOID_PREFIX) {
                if (thisPDN.getNote() != '') {
                    thisPDN.setValue(thisPDN.getNote().substr(5));
                    thisPDN.clearNote();
                    thisPDC.clearNote();
                }
            } // if the status was reset from void


        } // end if status column has been edited


    } // end fn:matEdit

    function matCleanup() {

        matDropDowns();
        matDateFormat();
        matNumberFormat();
        matSetFormulae();
        //  matSH.sort(col(H_PONUM).n);

    } // end fn:matCleanup

    function matDropDowns() {

        // get dropdowns from CentralData
        var CDD = getCentralDropDowns();

        // get suppliers from central and local 
        var allSuppliers = getSupplier('*all*');
        var supplierNames = [];
        for (; allSuppliers.length > 0;) {
            supplierNames.unshift(allSuppliers.pop()[0]);
        }
        // remove the header from the central data list
        supplierNames.shift();

        var pdnDDSource = CDD.ddPDN; // set the Project Dimension dd as all PDNs
        // if there is a budget, limit the PDNs to only those with a budgeted value
        /*  if(SS.getRangeByName("BudgetTotal").getValue()>0){
            pdnDDSource = []; // clear out the source
            var budgetSubmitted = SS.getRangeByName("BudgetSubmitted").getValues();
            var budgetPDCs = SS.getRangeByName("BudgetPDCs").getValues();

            for (var i=0;i<budgetPDCs.length;i++){ // loop through pdc column on the budget sheet
              var pdcIndex = CDD.ddPDC.indexOf(budgetPDCs[i][0]); // check if the the budget pdc exists 
              if (pdcIndex>-1  && budgetSubmitted[i][0] > 0){
                pdnDDSource.push(CDD.ddPDN[pdcIndex]);
              }// if this is a valid PDC and has a budgeted value
            }// end loop through budget PDCs    
          }// if there is a budget value
          pdnDDSource.push(CDD.ddPDN.pop()); // add back in the last entry ("void") from the centrall dropdown
        */
        var columns = [ // an array of columnHeader and dropDown datasource pairs
            H_TYPE, CDD.ddType
            , H_TEAM, CDD.ddTeams
            , H_PUOM, CDD.ddUoM
            , H_BUOM, CDD.ddUoM
            , H_PDN, pdnDDSource
            , H_VATRATE, CDD.ddVATRates
            , H_SUPPLIER, supplierNames
            , H_EMERGENCY, ['Yes', 'No']
            , H_STATUS, CDD.ddStatusBPR
            , H_HIRESTATUS, CDD.ddStatusHire
        ];

        for (; columns.length > 0;) {
            var thisCol = columns.shift(); // the column header (first of the array pair)
            var colRange = col(thisCol).a + (MHRI + 2) + ':' + col(thisCol).a; // the column reference in the for A1:A
            var dv = SpreadsheetApp.newDataValidation();
            dv.setAllowInvalid(false);
            var _dd = columns.shift(); // the dropdown source datalist, (second of the array pair)
            dv.requireValueInList(_dd, true);
            SS.getSheetByName(MATERIALS_SHEET).getRange(colRange).setDataValidation(dv.build());
        } // end for loop

    } // end fn:matDropDowns

    function matDateFormat() {

        /* * * * *
         * column headers to format as date/time 
         * * * * */
        var dtColumns = [
            col(H_ACTDEL).a
            , col(H_OFF).a
            , col(H_POCREATED).a
        ];

        for (; dtColumns.length > 0;) {
            var _col = dtColumns.pop();
            var fRange = matSH.getRange(_col + matFirst + ":" + _col + matLast);
            var fDT = []; // an array of number formats for datetime
            var fD = []; // an array of number formats for just the date

            for (var i = fRange.getNumRows(); i > 0; i--) {
                // push format inside of [] so that fDT and fD become a 2d object/array (rows x 1col)
                fDT.push(["dd/MM/yyyy HH:mm:ss"]);
                fD.push(["dd/MM/yyyy"]);
            }
            fRange.setNumberFormats(fDT);
            fRange.setNumberFormats(fD);
            fRange.setDataValidation(SpreadsheetApp.newDataValidation().requireDate().build());
        } // end for loop 

    } // end fn:matDateFormat


    function matNumberFormat() {

        /* * * * *
         * column headers to format
         * * * * */
        var colNumFormat = [ // an array of columnHeader and formatString pairs
            col(H_ICODE).a, '@STRING@'
            , col(H_UNIT).a, '0.00'
            , col(H_NET).a, '0.00'
            , col(H_VATRATE).a, '0%'
            , col(H_VATVALUE).a, '0.00'
            , col(H_LINEVALUE).a, '0.00'
        ];

        for (; colNumFormat.length > 0;) {
            var _col = colNumFormat.shift();
            var _format = colNumFormat.shift();
            var fRange = matSH.getRange(_col + matFirst + ":" + _col + matLast);
            var fNum = []; // an array of number formats 2 dp number

            for (var i = fRange.getNumRows(); i > 0; i--) {
                // push format inside of [] so that fNum become a 2d object/array (rows x 1col)
                fNum.push([_format]);
            }
            fRange.setNumberFormats(fNum);
        } // end for loop

    } // end fn:matNumberFormat

    function matSetFormulae() {

        var formulaColumns = [
            col(H_NET).a
            , '=IF(AND(EQ(' + col(H_QTY).a + matFirst + ',""),EQ(' + col(H_UNIT).a + matFirst + ',"")),"",' + col(H_QTY).a + matFirst + '*' + col(H_UNIT).a + matFirst + ')'

            , col(H_VATVALUE).a
            , '=IF(EQ(' + col(H_VATRATE).a + matFirst + ',""),"",' + col(H_NET).a + matFirst + '*(RIGHT(' + col(H_VATRATE).a + matFirst + ',(LEN(' + col(H_VATRATE).a + matFirst + ')-2))))'

            , col(H_LINEVALUE).a
            , '=IF(AND(EQ(' + col(H_NET).a + matFirst + ',"")),"",' + col(H_NET).a + matFirst + '+' + col(H_VATVALUE).a + matFirst + ')'

            , col(H_PDC).a
            , '=IF(EQ(' + col(H_PDN).a + matFirst + ',""),"",VLOOKUP(' + col(H_PDN).a + matFirst + ',PD_Range,2,FALSE))'

            , col(H_QTYLEFT).a
            , '=IF(EQ(' + col(H_QTY).a + matFirst + ',""),"",' + col(H_QTY).a + matFirst + '-' + col(H_QTYRCVD).a + matFirst + ')'
        ];

        // set the initial row formulae, then copy to the rest of the column
        for (; formulaColumns.length > 0;) {
            var initCol = formulaColumns.shift();
            var initRange = matSH.getRange(initCol + matFirst);
            initRange.setFormula(formulaColumns.shift());
            initRange.copyTo(matSH.getRange(initCol + (matFirst + 1) + ":" + initCol + matLast));
        }

    } // end fn:matSetFormulae

    var hRow = null;
    // function to return the column letter based on the header
    function col(columnHeader) {
        // import header row, only if not previously imported during the scope of this script call
        if (!hRow) { hRow = matSH.getRange((MHRI + 1), 1, 1, matSH.getLastColumn()).getValues(); }
        var hIndex = hRow[0].indexOf(columnHeader);
        if (hIndex > -1) {
            var retVal = { i: hIndex, n: (hIndex + 1), a: alphaCols[hIndex] };
            return retVal;
        }
    } // end fn:col


    function updateStatustoSent() {

        UI = SpreadsheetApp.getUi();

        // thisRowIndex is the current row -1 to allow for zero index in arrays  
        var thisRowIndex = SH.getActiveCell().getRow() - matFirst;

        // if a cell is selected in a header row, not an item row
        if (thisRowIndex <= MHRI) {
            var wrongRow = UI.alert("Status Update Error",
                "You have currently selected one of the header rows."
                + "\nPlease select a cell in a row from the materials list.",
                UI.ButtonSet.OK);
            return false;
        }

        // if not on the materials sheet
        if (SH.getName() != MATERIALS_SHEET) {
            var uiResponse = UI.alert("Status Update Error",
                "You are not on the " + MATERIALS_SHEET + " sheet.",
                UI.ButtonSet.OK);
            return false;
        }

        // get the PR numbers and Status Columns

        var statusBPRReady = '3 BPR Prepared';
        var statusBPRSent = '4 BPR Sent';

        var mtPRNoRange = matSH.getRange(matFirst, col(H_PONUM).n, matLast, 1);
        var mtPRNoVals = mtPRNoRange.getValues();


        var mtStatusRange = matSH.getRange(matFirst, col(H_STATUS).n, matLast, 1);
        var mtStatusVals = mtStatusRange.getValues();

        var thisBPRNo = mtPRNoVals[thisRowIndex][0];
        var okToUpdate = true; // a bit to check whether the update is possible  

        for (var u = 0; u < mtPRNoVals.length; u++) {
            if (mtPRNoVals[u][0] == thisBPRNo) {
                if (mtStatusVals[u][0] != statusBPRReady) { okToUpdate = false; }
                if (mtStatusVals[u][0] == statusBPRReady) { mtStatusVals[u][0] = statusBPRSent; }
            } // end if this BPR is THIS BPR
            if (!okToUpdate) { break; }
        }// end loop

        if (!okToUpdate) {
            var uiResponse = UI.alert("Status Update Error",
                "Not all line items for BPR " + thisBPRNo + " have a status of \"" + statusBPRReady + "\".",
                UI.ButtonSet.OK);
            return false;
        }

        if (okToUpdate) {
            mtStatusRange.setValues(mtStatusVals);
        }


    } // end fn:updateStatustoSent    
}
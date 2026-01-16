'use strict'

var hours = ['6 am', '7 am', '8 am', '9 am', ' 10 am', ' 11 am', ' 12 pm', ' 1 pm', ' 2 pm', ' 3 pm', ' 4 pm', ' 5 pm', ' 6 pm', ' 7 pm'];
var hoursAndTot = [" ", '6:00am', '7:00am', '8:00am', '9:00am', ' 10:00am', ' 11:00am', ' 12:00pm', ' 1:00pm', ' 2:00pm', ' 3:00pm', ' 4:00pm', ' 5:00pm', ' 6:00pm', ' 7:00pm', "Daily Location Total"];
var branches = [];
var brHourlyTotArr = [];
var finalTotalCookies = 0;


function BranchData(name, minCus, maxCus, avgCookiesPerCus) {
    this.name = name;
    this.minCus = minCus;
    this.maxCus = maxCus;
    this.avgCookiesPerCus = avgCookiesPerCus;
    this.amountOfCookies = 0;
    this.cookiesPerHour = [];
    this.totCookies = 0;
    branches.push(this);
}

BranchData.prototype.getRandCus = function () {
    return Math.floor(Math.random() * (parseInt(this.maxCus) - parseInt(this.minCus)) + parseInt(this.minCus));
}

BranchData.prototype.getAmountOfCookies = function () {
    return parseFloat(this.avgCookiesPerCus) * this.getRandCus();
}

BranchData.prototype.hourlyCookies = function () {
    for (var counter = 0; counter < hours.length ; counter++) {
        this.cookiesPerHour[counter] = this.getAmountOfCookies();
        this.totCookies +=  this.cookiesPerHour[counter];
    }
    return this.cookiesPerHour;
    // console.log(this.cookiesPerHour);
    // console.log(this.totCookies);
}

// Initializing the table
function initializingTable() {
    var parentElement = document.getElementById('main');
    var article = document.createElement('article');
    parentElement.appendChild(article);

    var table = document.createElement('table');
    table.setAttribute("id", "table");
    article.appendChild(table);

    var hoursAndTotalTr = document.createElement('tr');
    table.appendChild(hoursAndTotalTr);
    for (var counter3 = 0; counter3 < hoursAndTot.length; counter3++) {
        var hoursAndTotalTc = document.createElement('th');
        hoursAndTotalTc.textContent = hoursAndTot[counter3];
        hoursAndTotalTr.appendChild(hoursAndTotalTc);
    }
}

// Render the data in the table
BranchData.prototype.render = function () {
    var parentTable = document.getElementById("table");
    var tableRow = document.createElement('tr');
    parentTable.appendChild(tableRow);

    var tableBrName = document.createElement('th');
    tableBrName.textContent = this.name;
    tableRow.appendChild(tableBrName);

    for (var counter2 = 0; counter2 < hours.length; counter2++) {
        var tableCookiesSold = document.createElement('td');
        tableCookiesSold.textContent = this.cookiesPerHour[counter2];
        tableRow.appendChild(tableCookiesSold);
    }

    var tableTotCookiesPerBr = document.createElement('td');
    tableTotCookiesPerBr.textContent = this.totCookies;
    tableRow.appendChild(tableTotCookiesPerBr);
}

function renderBottomRow() {
    for (var counter4 = 0; counter4 < hours.length; counter4++) {
        var totalBranchPerHour = 0;
        for (var counter5 = 0; counter5 < branches.length; counter5++) {
            totalBranchPerHour += branches[counter5].cookiesPerHour[counter4];
            // brHourlyTot[counter4] +=parseInttotalBranchPerHour; 
        }
        brHourlyTotArr.push(totalBranchPerHour);
    }
    // console.log(totalBranchPerHour);
    // console.log(brHourlyTotArr);

    for (var counter6=0; counter6<brHourlyTotArr.length; counter6++){
        finalTotalCookies += brHourlyTotArr[counter6];
    }
    // console.log(finalTotalCookies);
    
    var parentTable = document.getElementById("table");
    var tableRow = document.createElement('tr');
    parentTable.appendChild(tableRow);

    var tableHourlyTotal = document.createElement('th');
    tableHourlyTotal.textContent = "Hourly Total";
    tableRow.appendChild(tableHourlyTotal);

    for (var counter7 = 0; counter7 < hours.length; counter7++) {
        var tableHourlyCookiesSold = document.createElement('td');
        tableHourlyCookiesSold.textContent = brHourlyTotArr[counter7];
        tableRow.appendChild(tableHourlyCookiesSold);
    }

    var tableFinalTotalCookies = document.createElement('td');
    tableFinalTotalCookies.textContent = finalTotalCookies;
    tableRow.appendChild(tableFinalTotalCookies);
}

initializingTable();

var Seattle = new BranchData("Seattle", 23, 65, 10);
var Tokyo = new BranchData("Tokyo", 3, 24, 2);
var Dubai = new BranchData("Dubai", 11, 38, 4);
var Paris = new BranchData("Paris", 20, 38, 6);
var Lima = new BranchData("Lima", 2, 16, 3);


for (var index = 0; index<branches.length;index ++){
    branches[index].hourlyCookies();
    branches[index].render();
}
// Seattle.hourlyCookies();
// Seattle.render();
// // console.log(Seattle);

// Tokyo.hourlyCookies();
// Tokyo.render();

// // console.log(Tokyo);

// Dubai.hourlyCookies();
// Dubai.render();
// // console.log(Dubai);

// Paris.hourlyCookies();
// Paris.render();
// // console.log(Paris);

// Lima.hourlyCookies();
// Lima.render();
// // console.log(Lima);

var branchForm = document.getElementById("addBranchForm")
branchForm.addEventListener('submit', function (event) {
    event.preventDefault();
    // console.log(event.target.location.value);
    var newBrName = event.target.location.value;
    var newMinCus = event.target.minimumCus.value;
    var newMaxCus = event.target.maximumCus.value;
    var newAvgCookies = event.target.averageCookies.value;

    // console.log(newBrName);
    // console.log(newMaxCus);
    // console.log(newMinCus);
    // console.log(newAvgCookies);

    var newBranch = new BranchData(newBrName, newMaxCus, newMinCus, newAvgCookies);
    // console.log(newBranch);
    
    var removeTable = document.getElementById("table");
    removeTable.remove();
    
    initializingTable();

    for (var counter8 = 0;counter8<branches.length;counter8++){
        branches[counter8].hourlyCookies();
        branches[counter8].render();
    }
    renderBottomRow();
    // Seattle.hourlyCookies();
    // Seattle.render();
    // Tokyo.hourlyCookies();
    // Tokyo.render();
    // Dubai.hourlyCookies();
    // Dubai.render();
    // Paris.hourlyCookies();
    // Paris.render();
    // Lima.hourlyCookies();
    // Lima.render();

    // newBranch.hourlyCookies();
    // newBranch.render();

    // renderBottomRow();

});

renderBottomRow();
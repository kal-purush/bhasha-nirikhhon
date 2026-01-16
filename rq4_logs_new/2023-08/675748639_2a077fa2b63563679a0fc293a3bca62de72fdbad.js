



function waterConversions(event) {
    let ttlwater = 0;
    let ttlcost = 0;
    if (event.eventType == 21) { // shower
        ttlwater = 1.8 * event.eventDuration
        ttlcost = .018 * event.eventDuration
    } else if (event.eventType == 13) { // dishwasher 
        ttlwater = .13 * event.eventDuration
        ttlCost = .0013 * event.eventDuration
    } else if (event.eventType == 17) { // washing machine
        ttlwater = .26 * event.eventDuration
        ttlCost = .0026 * event.eventDuration
    } else if (event.eventType == 20) { // faucet
        ttlwater = 1.5 * event.eventDuration
        ttlCost = .15 * event.eventDuration
    }

    
    return [ttlwater, ttlCost];
}


function powerConversion(event) {
    let ttlpower = 0;
    let ttlcost = 0;
   if (event.eventType == 7) { // overhead 4 lightbulb
        ttlpower = .00132 * event.eventDuration
        ttlCost = .000163812 * event.eventDuration
    } else if (event.eventType == 5 || event.eventType == 6) { // lamp 1 bulb
        ttlpower = .00033 * event.eventDuration
        ttlCost = .000040953 * event.eventDuration
    } else if (event.eventType == 19) { // room light 5 bulbs
        ttlpower = .00165 * event.eventDuration
        ttlCost = .000204765 * event.eventDuration
    } else if (event.eventType == 10) { // oven
        ttlpower = .04 * event.eventDuration
        ttlcost = .004964 * event.eventDuration
    } else if (event.eventType == 9) { // stove top
        ttlpower = .0125 * event.eventDuration
        ttlCost = .00155125 * event.eventDuration
    } else if (event.eventType == 22) { // thermostat/ac unit
        ttlpower = .05 * event.eventDuration
        ttlCost = .06205 * event.eventDuration
    } else if (event.eventType == 12) { // refridgerator
        ttlpower = .0029965753 * event.eventDuration
        ttlCost = .0003787 * event.eventDuration
    } else if (event.eventType == 18) { // dryer
        ttlpower = .019166 * event.eventDuration
        ttlCost = .002379 * event.eventDuration
    } else if (event.eventType == 4) { // television
        ttlpower = .000203576865 * event.eventDuration
        ttlCost = .0000252639 * event.eventDuration
    }  else if (event.eventType == 1 || event.eventType == 2 || //window
                event.eventType == 3 ){
            ttlpower = .005;
            ttlcost = .0012;
    } else if (event.eventType == 14 ||event.eventType == 15 || //door
        event.eventType == 16){
            ttlpower = .002;
            ttlcost = .0008;
    }
    
    return [ttlpower, ttlcost];

}

//global variables:
let waterUsage = new Array(31).fill(0);
let electrcityUsage = new Array(31).fill(0);
let totalCostForWaterAndElectricty = new Array(31).fill(0);
let predictedWaterUsage = new Array(31).fill(0);
let predictedElectrcityUsage = new Array(31).fill(0);
let predictedTotalCostForWaterAndElectricty = new Array(31).fill(0);
let graph = [];
let currentGraph = []
let data = {};


const getData = async function(){
    const response = await fetch("/data");
    data = await response.json();
    console.log(data);
    console.log("Month=> " ,monthNum);
    graphCurrentMonth(monthNum, data);
    dataToCards();
    totalAmount_cost_power_water(data);
} 

const listOfEventsPerMonth = function(month, event){
    waterUsage = new Array(31).fill(0);
    electrcityUsage = new Array(31).fill(0);
    totalCostForWaterAndElectricty = new Array(31).fill(0);
    const waterEvents = event.water[month]; // list of all water events within a single month
    const electrcityEvents = event.electrcity[month];// list of all electrcity events within a single month
 

    // // loop through water events:
    // console.log("Look:",waterEvents);
    if (waterEvents  == undefined) return;
    if (electrcityEvents == undefined) return;
    for (let i=0; i< waterEvents.length;i++){
        const waterEvent = waterEvents[i];
        const current__date = waterEvent.eventDate;
        const dateValue = parseInt(current__date.substring(current__date.length-2));
        let index = dateValue -1;
        const totalUseAndCost = waterConversions(waterEvent);
        waterUsage[index] += totalUseAndCost[0];
        totalCostForWaterAndElectricty[index] += totalUseAndCost[1];
    }

   

    // // loop through electrcity events:
    for (let i=0; i< electrcityEvents.length;i++){
        const electrcityEvent = electrcityEvents[i];
        const current__date = electrcityEvent.eventDate;
        const dateValue = parseInt(current__date.substring(current__date.length-2));
        let index = dateValue -1;
        const totalUseAndCost = powerConversion(electrcityEvent);
        electrcityUsage[index] += totalUseAndCost[0];
        totalCostForWaterAndElectricty[index] += totalUseAndCost[1];
        
    }

    // console.log(waterUsage,electrcityUsage,totalCostForWaterAndElectricty);
    graph = [];
    graph.push(['Days', 'Electricity','WaterUsage', "Cost For Both"]);

    for(let i=0;i<waterUsage.length;i++){
        graph.push([i+1, electrcityUsage[i], waterUsage[i],totalCostForWaterAndElectricty[i]]);
        // if i >= currentDay: currentGraph.push(...);
    }

    google.charts.load('current', {'packages':['corechart']});
    google.charts.setOnLoadCallback(drawChart);
    console.log(graph);

}


const graphCurrentMonth = function(currentMonth, event){
    waterUsage = new Array(31).fill(0);
    electrcityUsage = new Array(31).fill(0);
    totalCostForWaterAndElectricty = new Array(31).fill(0);
    predictedWaterUsage = new Array(31).fill(0);
    predictedElectrcityUsage = new Array(31).fill(0);
    predictedTotalCostForWaterAndElectricty = new Array(31).fill(0);
    const waterEvents = event.water[currentMonth]; // list of all water events within a single month
    const electrcityEvents = event.electrcity[currentMonth];// list of all electrcity events within a single month
 

    // // loop through water events:
    // console.log("Look:",waterEvents);
    if (waterEvents  == undefined) return;
    if (electrcityEvents == undefined) return;
    for (let i=0; i< waterEvents.length;i++){
        const waterEvent = waterEvents[i];
        const current__date = waterEvent.eventDate;
        const dateValue = parseInt(current__date.substring(current__date.length-2));
        let index = dateValue -1;
        const totalUseAndCost = waterConversions(waterEvent);
        waterUsage[index] += totalUseAndCost[0];
        totalCostForWaterAndElectricty[index] += totalUseAndCost[1];
    }

   

    // // loop through electrcity events:
    for (let i=0; i< electrcityEvents.length;i++){
        const electrcityEvent = electrcityEvents[i];
        const current__date = electrcityEvent.eventDate;
        const dateValue = parseInt(current__date.substring(current__date.length-2));
        let index = dateValue -1;
        const totalUseAndCost = powerConversion(electrcityEvent);
        electrcityUsage[index] += totalUseAndCost[0];
        totalCostForWaterAndElectricty[index] += totalUseAndCost[1];
        
    }

    // console.log(waterUsage,electrcityUsage,totalCostForWaterAndElectricty);
    graph = [];
    currentGraph = []
    graph.push(['Days', 'Electricity','WaterUsage', "Cost For Both"]);
    currentGraph.push(['Days', 'Electricity','WaterUsage', "Cost For Both", "predicted electricty", "predicted waterUsage", "predicted cost"]);

    let date = new Date();
    let currentDay = date.getDate()-1;
    let predWater = 0;
    let predElectrcity = 0;
    let predTotal = 0;

    for(let i=0;i<waterUsage.length;i++){
        graph.push([i+1, electrcityUsage[i], waterUsage[i],totalCostForWaterAndElectricty[i]]);
        if (i >= currentDay) {
            if (predWater== 200) {
                predWater = 150;
                predElectrcity = 5;
                predTotal = 18.5;
            }
            else {
                predWater = 200;
                predElectrcity = 8;
                predTotal = 20.4;
            }
            
            currentGraph.push([i+1, electrcityUsage[i], waterUsage[i],totalCostForWaterAndElectricty[i], predElectrcity,predWater, predTotal]);
        }
        else currentGraph.push([i+1, electrcityUsage[i], waterUsage[i],totalCostForWaterAndElectricty[i],0,0,0]);
    }

    google.charts.load('current', {'packages':['corechart']});
    google.charts.setOnLoadCallback(drawCurrentMonthChart);
    console.log(graph);

}






const totalAmount_cost_power_water = function(data){
    let power = 0;
    let water = 0;
    let cost = 0;

    const electrictylst = data.electrcity;
    const waterlst = data.water;

    
    electrictylst.forEach((month)=>{
        month.forEach((events)=>{
            const totalUseAndCost = powerConversion(events);
            power += totalUseAndCost[0];
            cost += totalUseAndCost[1];
            
        })
    });


    waterlst.forEach((month)=>{
        month.forEach((events)=>{
            const totalUseAndCost = waterConversions(events);
            water += totalUseAndCost[0];
            cost += totalUseAndCost[1];
            
        })
    });

    var waterCard = document.querySelector("#totalWaterCard .pwcData");
    waterCard.innerHTML = Math.floor(water) + " gal"
    waterCard.style.fontSize = "30px";
    var powerCard = document.querySelector('#totalPowerCard .pwcData');
    powerCard.innerHTML = Math.floor(power) + "kw"
    powerCard.style.fontSize = "30px";
    var costCard = document.querySelector('#totalCostCard .pwcData');
    costCard.innerHTML = "$ " + Math.floor(cost)
    costCard.style.fontSize = "30px";
  

}

// totalAmount_cost_power_water(data);










////////////////////////////////////////////////////////////////////////////
/////////////////////////// THE ARRAYS ////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

var months = [[1, "Jan.", "January", 31], // [key, month abbreviation, full month, how many days there are in each month]
             [2, "Feb.", "February", 28], 
             [3, "Mar.", "March", 31], 
             [4, "Apr.", "April", 30], 
             [5, "May", "May", 31], 
             [6, "June", "June", 30], 
             [7, "July", "July", 31], 
             [8, "Aug.", "August", 31], 
             [9, "Sept.", "September", 30], 
             [10, "Oct.", "October", 31], 
             [11, "Nov.", "Novemebr", 30], 
             [12, "Dec.", "Decemeber", 31]]

///////////////////////////////////////////////////////////////////////////
/////////////////////////// THE DATES ////////////////////////////////////
/////////////////////////////////////////////////////////////////////////

var date = new Date()
var monthNum = date.getMonth() // displays the current month (as a number)
let current__month = date.getMonth();
var fullYear = date.getFullYear() //displays the current year
var dateNum = date.getDate() // displays the current date of the month


console.log(months[date.getMonth()][2])
console.log(date.getFullYear()) 
console.log(date)

const allMonths = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec"
}


/// Within Total Section display November to Current Month:
const totalDate = document.getElementById("totalDate");
totalDate.innerHTML = `Nov. 2021 - ${allMonths[monthNum+1]} ${fullYear}`;
totalDate.style.color = "white";
totalDate.style.fontSize = "36px";






///////////////////////////////////////////////////////////////////////////
///////////////////////// Display Graph //////////////////////////////////
/////////////////////////////////////////////////////////////////////////
getData();
// listOfEventsPerMonth(monthNum, data);

///////////////////////////////////////////////////////////////////////////
///////////////////////// THE FUNCTIONS //////////////////////////////////
/////////////////////////////////////////////////////////////////////////

// changes the date of the graph title and html header
function changeGraphTitle() {
    graphTitle = months[monthNum][2] + " " + fullYear
    var docMonth = document.getElementById("month")
    docMonth.innerHTML = months[monthNum][1] + " " + fullYear
}

// left button shows the graph of the previous month
document.getElementById("leftBtn").addEventListener("click", function() {
    if (monthNum == 0) { // if the month is January, go to December when button is clicked
        monthNum = 12
    }
    monthNum = monthNum-1
    if (monthNum == 11) { //if the month is December, subtract the year by 1
        fullYear -= 1
    }
    changeGraphTitle()
    // console.log(months[monthNum])
    // console.log(fullYear)

    //I Added This:
    if (monthNum == current__month ) graphCurrentMonth(monthNum,data);
    else listOfEventsPerMonth(monthNum,data);
    dataToCards();
    // console.log(data);

    
})

// right button shows the graph of the months up to the current month
document.getElementById("rightBtn").addEventListener("click", function() {
    if (monthNum != date.getMonth() || fullYear != date.getFullYear()) {

        monthNum = monthNum+1

        if (monthNum == 12) { // if December is the month and the button is clicked, go to January
            monthNum = 0
        }

        if (monthNum == 0) { // if the month is January, add 1 to the year
            fullYear += 1
            
        }
 
    } else { // if the current month is being displayed, the user can't go to data for future months
        monthNum = date.getMonth() 
        fullYear = date.getFullYear()
    }
    changeGraphTitle()
    // console.log("Month: ", monthNum);
    if (monthNum == current__month ) graphCurrentMonth(monthNum,data);
    else listOfEventsPerMonth(monthNum,data);
    dataToCards();
    // console.log(months[monthNum])
    // console.log(fullYear)
})

// takes data from events and pushes them to the graph UI







// pushes the data from graphData to the pwc cards
function dataToCards() {
    var water = 0;
    var power = 0;
    var cost = 0;
    // console.log(graph);
    
    for (var x = 1; x < graph.length; x++) {
       

       
    
        power +=  Math.floor((graph[x][1]));
        water += Math.floor(graph[x][2]);
        cost +=  Math.floor(graph[x][3]);
    }
    console.log("Update: ", power,water,cost);
    var waterCard = document.getElementById("waterdata")
    waterCard.innerHTML = water + " gal"
    var powerCard = document.getElementById('powerdata')
    powerCard.innerHTML = power + "kw"
    var costCard = document.getElementById('costdata')
    costCard.innerHTML = "$ " + cost
}
///////////////////////////////////////////////////////////////////////////
/////////////////////////// THE GRAPH ////////////////////////////////////
/////////////////////////////////////////////////////////////////////////

// google.charts.load('current', {'packages':['corechart']});
// google.charts.setOnLoadCallback(drawChart);

function drawChart() {
//   var data = google.visualization.arrayToDataTable(graphData);
var data = google.visualization.arrayToDataTable(graph);
 console.log(graph);

  var options = {
    title: graphTitle,
    curveType: 'function',
    // chartArea:{left:40,top:50,width:'90%',height:'80%'},
    legend: { position: 'bottom' },
    series: {
        0: {color: 'orange'},
        1: {color: 'lightblue'},
        2: {color: 'red'},
    }
  };

  var chart = new google.visualization.LineChart(document.getElementById('curve_chart'));
  chart.draw(data, options);
}



function drawCurrentMonthChart() {
    //   var data = google.visualization.arrayToDataTable(graphData);
    var data = google.visualization.arrayToDataTable(currentGraph);
    //  console.log(graph);
    
      var options = {
        title: graphTitle,
        curveType: 'function',
        // chartArea:{left:40,top:50,width:'90%',height:'80%'},
        legend: { position: 'bottom' },
        series: {
            0: {color: 'orange'},
            1: {color: 'lightblue'},
            2: {color: 'red'},
            3: {color: 'orange', lineDashStyle: [4, 4]},
            4: {color: 'lightblue', lineDashStyle: [4, 4]},
            5: {color: 'red', lineDashStyle: [4, 4]},
        }
      };
    
      var chart = new google.visualization.LineChart(document.getElementById('curve_chart'));
      chart.draw(data, options);
    }

changeGraphTitle()


dataToCards();





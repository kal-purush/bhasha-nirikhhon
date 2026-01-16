// from data.js
var tableData = data;
console.log(tableData,0);

// Cleaning data up a little bit
// this data is messed up, so we could do a lot more
function dataClean(){
    for(i=0;i<tableData.length; i++){
        tableData[i].country = tableData[i].country.toUpperCase();
        tableData[i].state = tableData[i].state.toUpperCase();
        
        // attempt to fix () messiness
        var cityLower = tableData[i].city;
        var firstParan = cityLower.indexOf("(")
        var closeParan = cityLower.indexOf(")")
        while (firstParan > -1){
            if (closeParan > -1){
                cityLower = cityLower.slice(0, firstParan) + cityLower.slice(closeParan + 1)
                // console.log(cityLower)
                firstParan = cityLower.indexOf("(")
                closeParan = cityLower.indexOf(")")
            } else {
                cityLower.replace("(", " ")
                // console.log(cityLower)
            }
        }
        // capitalize each word in city
        var cityLowerArray = cityLower.split(" ");
        var cityUpperArray = []
        for (j=0; j<cityLowerArray.length; j++){
                var capitalizedWord = cityLowerArray[j].slice(0,1).toUpperCase() + cityLowerArray[j].slice(1);
                cityUpperArray.push(capitalizedWord)
        }
        var cityUpper = cityUpperArray.join(" ")
        // console.log(cityUpper)
        tableData[i].city = cityUpper.trim() 
        
        //  Clean Shape name
        var shapeName = tableData[i].shape
        var shapeUpper = shapeName.slice(0,1).toUpperCase() + shapeName.slice(1);
        tableData[i].shape = shapeUpper;
    }
  };
             
  dataClean();

// Use D3 to select the table body
var tbody = d3.select("tbody");

// Loop through tableData and build the table
// 

tableData.forEach((sighting) => {
    var row = tbody.append("tr");
    Object.entries(sighting).forEach(([key, value]) => {
      var cell = tbody.append("td");
      cell.text(value);
    });
  });

var submit = d3.select("#filter-btn");
  
submit.on("click", function() {

    // Prevent the page from refreshing
    d3.event.preventDefault();

    // clear the original table
    d3.select("tbody").html("");
    d3.select("#filterMsg").html("");

    var filtersUsed = []
    var filteredData = tableData;
    // ************  COLLECT VALUES FROM ALL FILTERS
    // Select the input element for datetime, city, state, country 
    // get value
    var inputDatetime = d3.select("#datetime");
    var datetimeValue = inputDatetime.property("value");

    var inputCity = d3.select("#city");
    var cityValue = inputCity.property("value");

    var inputState = d3.select("#state");
    var stateValue = inputState.property("value");

    var inputCountry = d3.select("#country");
    var countryValue = inputCountry.property("value");

    var inputShape = d3.select("#shape");
    var shapeValue = inputShape.property("value");

    // ********* Conditional to check filters
    if (datetimeValue) {
        // Create variable to hold first filter criterion
        var filteredData = filteredData.filter(sighting => sighting.datetime === datetimeValue);
        filtersUsed.push(["Date "]);
    }
    if (cityValue) {
            // Create variable to hold first filter criterion
            filteredData = filteredData.filter(sighting => sighting.city === cityValue);
            filtersUsed.push(["City "]);        
    }
    if (stateValue) {
        // Create variable to hold first filter criterion
        filteredData = filteredData.filter(sighting => sighting.state === stateValue);
        filtersUsed.push(["State "]);        
    }
    if (countryValue) {
        // Create variable to hold first filter criterion
        filteredData = filteredData.filter(sighting => sighting.country === countryValue);
        filtersUsed.push(["Country "]);        
    }
    if (shapeValue) {
        // Create variable to hold first filter criterion
        filteredData = filteredData.filter(sighting => sighting.shape === shapeValue);
        filtersUsed.push(["Shape "]);        
    }

    // create a msg to let the user know which filter is applied
    // first check to see if any filters are used
    if (filtersUsed.length === 0) {
        d3.select("#filterMsg").text("No filters used");
    }

    else {
        d3.select("#filterMsg").append("span").
            text("Filters Used: ");
        filtersUsed.forEach((filter) =>  {
        
        Object.entries(filter).forEach(([key, value]) =>{
            var entry = d3.select("#filterMsg").append("span");
            entry.text(value);
            });
        });
    }

    // create the new table using filters
    filteredData.forEach((sighting) => {
        var row = tbody.append("tr");
        Object.entries(sighting).forEach(([key, value]) => {
        var cell = tbody.append("td");
        cell.text(value);
        });
    });
});

// the is additional code to support the pretend
// input page which allows users to add a new sighting
// this creates the dictionary entry but does not have
// backend to write the data

var inputSubmit = d3.select("#input-btn");

// collects the data input from input.html file
inputSubmit.on("click", function() {
    
    // Prevent the page from refreshing
    d3.event.preventDefault();

    newSighting = [];

    newSighting.push({"datetime": d3.select("#formDatetime").node().value});
    newSighting.push({"city": d3.select("#formCity").node().value});
    newSighting.push({"state": d3.select("#formState").node().value});
    newSighting.push({"country": d3.select("#formCountry").node().value});
    newSighting.push({"shape": d3.select("#formShape").node().value});
    newSighting.push({"durationMinutes": d3.select("#formDuration").node().value});
    newSighting.push({"comments": d3.select("#formComment").node().value});

    console.log(newSighting);
    data.push(newSighting);
});
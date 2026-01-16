var api_key = "752b41def39260e71140c4018e9a0f11";
var cityName;
var queryURL;
var arrayOfCities = [];


$(document).ready(function() {

    startUp();

function startUp(){
    if (cityName == null)   {
        cityName = "raleigh";
    }
    cityName = localStorage.getItem('starter-city');
    queryURL = "http://api.openweathermap.org/data/2.5/weather?q=" + cityName + "&units=imperial&appid=" + api_key;

    updateForecast(); 
    fiveDaySchedule();
}

$("#city-button").on("click", function() {
    
    cityName = $("#search-city").val();
    queryURL = "http://api.openweathermap.org/data/2.5/weather?q=" + cityName + "&units=imperial&appid=" + api_key;

    updateForecast(); 
    fiveDaySchedule();
    updateCitiesList();
    
});

$("ul").on("click", "li", function() {

    cityName = $(this).text();
    console.log(this);
    queryURL = "http://api.openweathermap.org/data/2.5/weather?q=" + cityName + "&units=imperial&appid=" + api_key;

    updateForecast();
    fiveDaySchedule();
    updateCitiesList();

});

 
function updateCitiesList () {
    if (arrayOfCities.includes(cityName.toLowerCase())) {
        var index = arrayOfCities.indexOf(cityName);
        arrayOfCities.splice(index, 1);
    }
    arrayOfCities.unshift(cityName.toLowerCase());
    $(".list-of-cities").empty();
    while (arrayOfCities.length > 8) {
        arrayOfCities.pop();
    }
    for (var i = 0; i < arrayOfCities.length; i++) {
        var newCity = document.createElement("li");
        newCity.classList += "city-in-list list-group-item";
        newCity.id = "city-number" + i;
        document.getElementById("unordered-list-cities").appendChild(newCity);
        $("#"+newCity.id).append(arrayOfCities[i]);
    }
    localStorage.setItem('starter-city', cityName);
    
}


function updateForecast () {
    $.ajax({
        url: queryURL,
        method: "GET"
    }).then(function(pageInfo) {
        var temperature = pageInfo.main.temp;
        var humid = pageInfo.main.humidity;
        var windSpd = pageInfo.wind.speed;
        var long = pageInfo.coord.lon;
        var lati = pageInfo.coord.lat; 
        var nameOfCity = pageInfo.name;

        $(".temperature").empty();
        $(".temperature").append("Temperature: " + temperature + " °F");
        $(".humidity").empty();
        $(".humidity").append("Humidity: " + humid + "%");
        $(".wind-speed").empty();
        $(".wind-speed").append("Wind Speed: " + windSpd + " MPH");


        var queryURLuV = "http://api.openweathermap.org/data/2.5/uvi?lat=" + lati + "&lon=" + long + "&appid=752b41def39260e71140c4018e9a0f11"

        $.ajax({
            url: queryURLuV,
            method: "GET"
        }).then(function(pageInfo2) {
            var uV = pageInfo2.value;
            var dateInfo = pageInfo2.date_iso
            var imgOfIcon = pageInfo.weather[0].icon;

            var help = dateInfo.split("-");
            var uvId = document.getElementById("uv-index");

            if (uV <= 2)    {
                $("#uv-index").empty();
                $("#uv-index").removeClass();
                $("#uv-index").append("UV Index: "+ uV);
                uvId.classList.add("uv-low");
            } else if (uV > 2 && uV <= 7)    {
                $("#uv-index").empty();
                $("#uv-index").removeClass();
                $("#uv-index").append("UV Index: "+ uV);
                uvId.classList.add("uv-moderate");
            } else if (uV > 7)  {
                $("#uv-index").empty();
                $("#uv-index").removeClass();
                $("#uv-index").append("UV Index: "+ uV);
                uvId.classList.add("uv-high");
            } else {
                $("#uv-index").empty();
                $("#uv-index").removeClass();
                $("#uv-index").append("UV Index: could not be found");
            }


            var fullNameTitle = nameOfCity + " (" + help[1] + "/"+help[2].charAt(0) + help[2].charAt(1)+"/"+help[0]+")";

            $(".city-name").empty();
            $(".city-name").append(fullNameTitle);

            var imgText = "http://openweathermap.org/img/wn/" +imgOfIcon + "@2x.png";

            $(".image-of-weather").attr("src", imgText);





            

        });
    });
}

function fiveDaySchedule () {
    var urlFiveDay = "http://api.openweathermap.org/data/2.5/forecast?q=" + cityName + "&units=imperial&appid=" + api_key;
    $.ajax({
        url: urlFiveDay,
        method: "GET"
    }).then(function(pageInfo) {

        for (var i = 1; i <6; i++) {
            $("#daily-block"+i).remove();
        }
        
        for (var i = 1; i <6; i++) {

            //all of the variables for the week

            var dateOfWeather =pageInfo.list[8*i-1].dt_txt;
            dateElements = dateOfWeather.split("-");
            finalDate = "("+dateElements[1]+"/"+dateElements[2].charAt(0)+dateElements[2].charAt(1)+"/"+dateElements[0]+")";

            var imgOfIcon = pageInfo.list[(8*i-1)].weather[0].icon;
            var temperature = pageInfo.list[(8*i-1)].main.temp;
            var humid = pageInfo.list[(8*i-1)].main.humidity;

            //making new div to store all in

            var newDiv = document.createElement("div");
            newDiv.classList.add("five-day-block");
            newDiv.id = "daily-block" + i;
            document.getElementById("five-day-append").appendChild(newDiv);

            //Making the elements for the individual boxes

            var newDate = document.createElement("h5");
            newDate.classList += "date-block";
            newDate.id = "date-block" + i;
            document.getElementById(newDiv.id).appendChild(newDate);
            $("#"+newDate.id).append(finalDate);

            var newIcon = document.createElement("img");
            newIcon.classList += "weather-icon";
            newIcon.id = "weather-icon" + i;
            document.getElementById(newDiv.id).appendChild(newIcon);
            $("#"+newIcon.id).attr("src", "http://openweathermap.org/img/wn/" +imgOfIcon + "@2x.png");

            var newTemp = document.createElement("p");
            newTemp.classList += "temp-block";
            newTemp.id = "temp-block" + i;
            document.getElementById(newDiv.id).appendChild(newTemp);
            $("#"+newTemp.id).append("Temperature: " +temperature+" °F");

            var newHum = document.createElement("p");
            newHum.classList += "hum-block";
            newHum.id = "hum-block" + i;
            document.getElementById(newDiv.id).appendChild(newHum);
            $("#"+newHum.id).append("Humidity: " +humid + "%");

            
        }
    });
}

});
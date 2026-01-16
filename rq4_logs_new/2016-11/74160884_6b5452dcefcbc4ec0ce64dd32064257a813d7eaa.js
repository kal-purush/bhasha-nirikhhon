$(document).ready(function(){
    var lat = 0;
    var long = 0;
    
    navigator.geolocation.getCurrentPosition(successCallback,errorCallback,{maximumAge:600000});
        //Loads based on position if accepted
        function successCallback(position) {
            lat = position.coords.latitude;
            long = position.coords.longitude;
            console.log("Geo Set");
            retrieveData(lat,long);
        }
        
        //Loads default if denied
        function errorCallback(){
            //Default settings
            lat = 35;
            long = 139;
            $.get('http://ipinfo.io/json', function(data){
                console.log(data);
                var ispLocation = data['loc'];
                var arrayLocation = ispLocation.split(",");
                lat = arrayLocation[0];
                long = arrayLocation[1];
                console.log("Default");
                console.log(lat + "&" + long);
                retrieveData(lat,long);
            });
            
        }
    
});
/** Grabs data from site and returns json **/
function retrieveData(lat, long){
        $.post('weather.php',{lat: lat,long: long}, function(data){
            var weather = JSON.parse(data);
            console.log(weather);
            
            var cityCountry = weather['name'] +", "+weather['sys'].country;
            var temp = Math.round(weather['main'].temp);
            var humidity = weather['main'].humidity;
            var icon = weather["weather"][0].icon;
            //Changes color based on temperature
            changeTempColor(temp);
            
            $("#percentage").html(humidity+"%");
            $("#degree").html("<p id='temp'>"+temp + "&#176;<span id='convert'>F</span></p>");
            $("#description").html("<i class='wi " + convertIcon(icon)+"'></i>");
            $("p").on("click", function(){
                var selection = document.getElementById("convert").innerHTML;
                // Switch between Farhenheit and Celsius
                switch(selection){
                    case "F":
                        document.getElementById("temp").innerHTML = convertToCelsius(temp) + "&#176;<span id='convert'>C</span>";
                        break;
                    case "C":
                        document.getElementById("temp").innerHTML = temp + "&#176;<span id='convert'>F</span>";
                        break;
                }
            });
        });
    }
    
function convertToCelsius(degree) {
    return Math.round((degree-32)*(5/9));
}

function changeTempColor(value){
    if(value >= 100)
        $('#tempColor').addClass("hot");
    else if (value >= 80 && value < 100)
        $('#tempColor').addClass("warm");
    else if (value >= 60 && value < 80)
        $('#tempColor').addClass("mild");
    else if (value >= 40 && value < 60)
        $('#tempColor').addClass("cool");
    else if(value >= 33 && value < 40)
        $('#tempColor').addClass("cold");
    else if(value >= 0 && value < 33)
        $('#tempColor').addClass("freezing");
    else if (value < 0)
        $('#tempColor').addClass("belowZero");

}

function convertIcon(icon){
    console.log(icon);
    switch(icon){
        case "01d":
            //clear sky day
            return "wi-day-sunny";
            break;
        case "02d":
            //few clouds  day
            return "wi-day-cloudy";
            break;
        case "03d":
        case "04d":
        case "03n":
        case "04n":
            //broken clouds  day
            return "wi-cloudy";
            break;
        case "09d":
        case "10d":
            //rain day
            return "wi-day-rain";
            break;
        case "11d":
            //thunderstorm day
            return "wi-day-thunderstorm";
            break;
        case "13d":
            //snow day
            return "wi-day-snow";
            break;
        case "50d":
            //mist day
            return "wi-day-fog";
            break;
        case "01n":
            return "wi-night-clear";
            break;
        case "02n":
            return "wi-night-alt-cloudy";
            break;
        case "09n":
        case "10n":
            return "wi-night-alt-rain";
            break;
        case "11n":
            return "wi-night-alt-thunderstorm";
            break;
        case "13n":
            return "wi-night-alt-snow";
            break;
        case "50n":
            return "wi-night-fog";
            break;
    }
    return "wi-na";
}

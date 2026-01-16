$(document).ready(function() {

    //event listener for search button
    $("#submit").click(function() {
        event.preventDefault();

        let city = $("#city").val();
        let apiKey = "e5d47731dc26eefda896c92eb148db5f"
        let queryURL = "https://api.openweathermap.org/data/2.5/weather?q=" + city + "&appid=e5d47731dc26eefda896c92eb148db5f"
        
        //ajax call to search for city
        $.ajax({
            url: queryURL,
            method: "GET"
        }).then(function(response) {
            cityId = result.id;

                $.ajax({url: "api.openweathermap.org/data/2.5/weather?q={cityId}&appid={apiKey}&units=imperial",
                success: function(result) {
                    $("#current").html("");
                    $("#current").append("<div class='heading'><h1>${result.city.name} (${getDate(0)})</h1>");
                    $("#current").append("<p class='temperature'>Tempurature: ${result.list[0].main.temp} °F</p>");
                    $("#current").append("<p class='humidity'>Humidity: ${result.list[0].man.humidity} %</p>");
                    $("#current").append("<p class='windSpeed'>Wind Speed: ${mph(result.list[0].wind.speed)} MPH</p>");
                }

                })

        })
    })
})
$(document).ready(() => {
    function weatherInfo() {
        var city = $("#city-input").val()
        var apiQuery = "https://api.openweathermap.org/data/2.5/forecast?q=" + city + "&units=imperial&appid=caf932346b78dde952075fa3fc6aed80"
        console.log(city)

        $.ajax({
            url: apiQuery,
            method: "GET"
        }).then(function(res) {
            $(".city-name").text(res.city.name)
            $('#temperature').text("Temperature: " + res.list[0].main.temp)
            $('#wind-speed').text("wind-speed:" + res.list[0].wind.speed)
            $('#humidity').text("humidity:" + res.list[0].main.humidity)
            console.log(res)
        })
    }

    $("#search-button").on("click", weatherInfo)
})


// var queryURL = "https://cors-anywhere.herokuapp.com/http://api.openweathermap.org/data/2.5/forecast?q="+city+"&appid=caf932346b78dde952075fa3fc6aed80"
// $.ajax({
//     header: origin,
//     url: queryURL,
//     method:"GET"
// }).then(function(response){
//     for(var i = 3; i < 36; i=i+8){
//         var temp = (((1.8)*(response.list[i].main.temp-273)+32).toFixed(2)).toString()
//         var date = response.list[i].dt_txt.substring(0,10)
//         var weather = response.list[i].weather[0].icon
//         $(".date"+i).html(date)
//         $(".temp"+i).html("Temperature: " + temp + "°F")
//         $(".humidity"+i).html("Humidity: " + response.list[i].main.humidity + "%")
//         var icon = $("<img src = 'http://openweathermap.org/img/wn/" + weather + ".png'>")
//         $(".icon"+i).html("")
//         $(".icon"+i).append(icon)
//     }
//     console.log(weather)



// })
var getWeather = function() {
    var form = document.querySelector('.zip-code');
    if(form.value.length === 5) {
        var url = 'http://api.openweathermap.org/data/2.5/weather?zip='+form.value+',us&appid=92c053c5ea0c533117e198eab007baef';

        var jsonCallback = function (data) {
            var data = data;
            console.log(data);
            var celsius = data.main.temp - 273;
            var fahrenheit = Math.floor(celsius * (9/5) + 32);
            $('.weather').css('display','block');
            var div = $('.weather');
            div.empty();
            div.append('<span class="city">'+data.name+', '+data.sys.country+'</span>');
            div.append('<span class="temp">'+fahrenheit+'</span>');
            div.append('<span class="city">Humidity</span>');
            div.append('<span class="temp">'+data.main.humidity+' %</span>');
            div.append('<span class="city">Wind</span>');
            div.append('<span class="temp">'+data.wind.speed+' ml/s</span>');
            div.append('<span class="city">'+data.weather[0].main+'</span>');
            div.append('<span class="temp">'+data.weather[0].description+'</span>');
        };

        $.ajax({
            type: 'GET',
            url: url,
            contentType: "application/json",
            dataType: 'jsonp'
        }).done(jsonCallback).fail(function (xhr) {
            console.log("error" + xhr.responseText);
            $('.weather').css('display','block');
            var div = $('.weather');
            div.empty();
            div.append('<span class="error-mess">Wrong ZIP Code</span>');
        });
    }
};
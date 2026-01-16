angular.module('weatherApp', [])
  .controller('WeatherAppController', function($scope, $http) {
    let weathers = this;
    const months = {
      1: 'January',
      2: 'February',
      3: 'March',
      4: 'April',
      5: 'May',
      6: 'June',
      7: 'July',
      8: 'August',
      9: 'September',
      10: 'October',
      11: 'November',
      12: 'December'
    };
    const days = {
      0: 'Sunday',
      1: 'Monday',
      2: 'Tuesday',
      3: 'Wednesday',
      4: 'Thursday',
      5: 'Friday',
      6: 'Saturday'
    };

    $scope.weathers.apiurl = `http://api.openweathermap.org/data/2.5/forecast?zip=60661,us&APPID=50022cdd75839184d596bea5c3c632fe`;
    $scope.weathers.currentWeather = null;
    $scope.weathers.useDefault = false;
    $scope.weathers.forecast = [];
    if (navigator.geolocation) {
      navigator.geolocation.getCurrentPosition(getLongLat, err);
    } else {
      err(null);
    }

    function getLongLat(position) {
      $scope.weathers.apiurl = buildURL({
        latitude: position.coords.latitude,
        longitude: position.coords.longitude
      });
      makeApiCall($scope.weathers.apiurl);
    }

    function err(error) {
      console.warn(`User did not provide location data. Defaulting to 60661. Message: ${error.message}`);
      $scope.weathers.useDefault = true;
      makeApiCall($scope.weathers.apiurl);
    }

    function buildURL(latlon) {
      return `http://api.openweathermap.org/data/2.5/forecast?lat=${latlon.latitude}&lon=${latlon.longitude}&APPID=50022cdd75839184d596bea5c3c632fe`
    }

    function makeApiCall(apiurl) {
      $http({
          method: 'GET',
          url: apiurl
        })
        .then(successCallback, errorCallback);
    }

    function successCallback(response) {
      const date = buildForecastDate(response.data.list[0].dt_txt)
      $scope.weathers.currentWeather = {
        sky: response.data.list[0].weather[0].main,
        city: `${response.data.city.name}, ${response.data.city.country}`,
        date: date.date,
        day: date.day,
        temperature: ktof(response.data.list[0].main.temp)
      };
      for (let i = 0; i < response.data.list.length; i += 8) {
        const index = i;
        const info = response.data.list[index];
        const date = buildForecastDate(info.dt_txt);
        $scope.weathers.forecast.push({
          temperature: ktof(info.main.temp),
          date: date.date,
          day: date.day
        });
      }
      $scope.$apply();
    }

    function errorCallback(response) {
      console.error("Cannot get Weather Data");
    }

    function buildForecastDate(date) {
      const d = new Date(date);
      const month = months[d.getMonth()];
      return {day: days[d.getDay()], date: `${month} ${d.getDate()}, ${d.getFullYear()}`};
    }

    function ktof(kelvin) {
      return Math.round((kelvin * (9 / 5)) - 459.67);
    }
  });
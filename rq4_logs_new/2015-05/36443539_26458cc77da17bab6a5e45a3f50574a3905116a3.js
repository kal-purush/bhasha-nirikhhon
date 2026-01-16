/**
 * Created by paipeng on 29.05.15.
 */


var weatherModule = angular.module('weather2.Module', [
    'ui.router',
    'ngResource',
    'weather2.Service'
]);

weatherModule
    .config(['$stateProvider', '$urlRouterProvider',
        function ($stateProvider, $urlRouterProvider) {
            console.log("weather2.Module loaded " + weatherModule.requires);

            $stateProvider.state('weather', {
                url: '/weather',
                template: '<div ng-show="weatherList"> <h1>Weather List</h1> <table st-table="displayed" class="table table-striped"> <thead> <tr> <th st-ratio="20" st-sort="firstName">Country</th> <th st-ratio="20" st-sort="lastName">City</th> <th st-ratio="10" st-sort="age">Weather</th> <th st-ratio="30" st-sort="email">Temp</th> <th st-ratio="20" st-sort="balance">Diff</th> </tr> </thead> <tbody> <tr ng-repeat="row in weatherList"> <td st-ratio="20"><img src="http://img.fifa.com/images/flags/4/{{row[\'4\']}}.png"> {{row.country | uppercase}}</td> <td st-ratio="20">{{row.weather.name}}</td> <td st-ratio="10">{{row.weather.weather[0].description}}</td> <td st-ratio="10">{{row.weather.main.temp}}</td> <td st-ratio="20">{{getDiff(row["Total Points"], row["Previous Points"])}}</td> </tr> </tbody> <tfoot> <tr> <td colspan="5" class="text-center"> <div st-items-by-page="20" st-pagination=""></div> </td> </tr> </tfoot> </table></div>',
                controller: 'weatherController'
            });

        }
    ]).controller('weatherController', ['$rootScope', '$scope', 'Weather',
        function(rootScope, scope, Weather) {
            scope.list = true;

            scope.text = "hello 2";

            scope.weatherList = [];

            initWeatherList();

            function initWeatherList() {
                scope.countryList = [];
                if (rootScope.rankingList === undefined || rootScope.rankingList.size <= 0) {
                    scope.countryList.push({country: 'Germany'});
                    scope.countryList.push({country: 'China'});
                    scope.countryList.push({country: 'France'});

                    angular.forEach(scope.countryList, function(value, key) {
                        setWeatherForCountry(value.country);
                    });

                } else {
                    angular.forEach(rootScope.rankingList, function(value, key) {
                        setWeatherForCountry(value.Team);
                    })
                }
            };

            function setWeatherForCountry(country) {
                Weather.getCapitalByCountry(country).success(function(res) {
                    console.log('getCapitalByCountry res ' + JSON.stringify(res));
                    var city = res[0].capital;
                    Weather.getWeatherByCity(city).success(function (res) {
                        console.log('getWeatherByCity res ' + JSON.stringify(res));
                        scope.weatherList.push({country: country, weather: res});
                    })
                });
            };
        }
    ]);




angular.module('weather2.Service', [])
    .factory("Weather", ['$http', function($http) {

        //'use strict';

        var url = "https://restcountries.eu/rest/v1/name/";

        var weather_url = "http://api.openweathermap.org/data/2.5/weather?APPID=1935ce19500a433277db92080020ed37&q="

        function getCapitalByCountry(country) {
            return $http.get(url+country);
        }

        function getWeatherByCity(city) {
            return $http.get(weather_url+city).success(function(response) {
                return response.data;
            }).error(function(err) {
                return null;
            });
        }


        return {
            getWeatherByCity: function(city) {
                console.log("getWeatherByCity " + city);
                return getWeatherByCity(city);

            },
            getCapitalByCountry: function(country) {
                console.log("getCapitalByCountry " + country);
                return getCapitalByCountry(country);
            }

        }
    }]);

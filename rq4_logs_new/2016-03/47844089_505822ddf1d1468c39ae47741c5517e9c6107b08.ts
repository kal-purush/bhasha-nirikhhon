/// <reference path='../typings/tsd.d.ts' />

import request = require('request');
import qs = require('querystring');
import weatherKey = require('../config/config');

var currentWeatherEndpoint = 'http://api.openweathermap.org/data/2.5/weather?';
var forecastEndpoint = 'http://api.openweathermap.org/data/2.5/forecast?';
var dailyForecastEndpoint = 'http://api.openweathermap.org/data/2.5/forecast/daily?';


export class Weather {
    public getCurrentWeather(params, res) {
        this.queryOpenWeatherAPI(currentWeatherEndpoint + qs.stringify(params), res);
    }

    private queryOpenWeatherAPI(url, res) {
        request({
                uri: url + '&appid=' + weatherKey.WeatherKEY,
                method: 'GET'
            },
            function (error, response, body) {
                if (!error && response.statusCode === 200) {
                    res.status(200).send(body);
                } else {
                    console.error(error, body);
                    res.status(response.statusCode).send(body);
                }
            }
        );
    }
}
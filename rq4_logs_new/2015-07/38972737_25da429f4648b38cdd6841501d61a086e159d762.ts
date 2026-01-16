/// <reference path="../../typings/tsd" />

import angular = require('angular');
import ngRoute = require('angular-route');

angular.module('myApp', [ngRoute]);

import config = require('./config/config');

config();
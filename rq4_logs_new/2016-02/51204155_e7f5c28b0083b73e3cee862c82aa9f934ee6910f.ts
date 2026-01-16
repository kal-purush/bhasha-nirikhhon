import {angular} from 'angularts/angular';
import 'angularts/angular-route';
import 'angularts/angular-aria';
import 'angularts/angular-animate';
import 'angularts/angular-material';

module HaveIBeenPwned{
    console.log(angular);
    angular
        .module("HaveIBeenPwned", ["ngMaterial","ngRoute"]);
}
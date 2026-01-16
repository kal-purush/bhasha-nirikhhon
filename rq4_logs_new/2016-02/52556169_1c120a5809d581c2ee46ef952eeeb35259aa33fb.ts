import * as angular from 'angular';
import {TodoCtrl} from 'controllers/todoCtrl';

module todo {
    angular.module('TodoApp', [])
        .controller('TodoCtrl', TodoCtrl);

    angular.bootstrap(document, ['TodoApp']);
}
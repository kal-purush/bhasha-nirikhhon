/// <reference path="../../../../typings/tsd.d.ts" />

interface Phone {
    name: string;
    snippet: string;
}

module Controller.PhonecatApp {
    export class PhoneListController {
        public phones:Phone[]

        constructor () {
            this.phones = [
                {
                    'name': 'Nexus S',
                    'snippet': 'Fast just got faster with Nexus S.'
                },
                {
                    'name': 'Motorola XOOM™ with Wi-Fi',
                    'snippet': 'The Next, Next Generation tablet.'
                },
                {
                    'name': 'MOTOROLA XOOM™',
                    'snippet': 'The Next, Next Generation tablet.'
                }
            ];
        }

        static init () {
            var phonecatApp = angular.module('PhonecatApp', []);
            phonecatApp.controller('PhoneListController', [PhoneListController]);
        }
    }
}
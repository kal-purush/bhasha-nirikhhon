import {CORE_DIRECTIVES} from 'angular2/common';
import {Component} from 'angular2/core';

import {MonyeDirective} from "./money";
import {DatetimeDirective} from "./datetime";

@Component({
    selector: 'app',
    templateUrl: 'app/app.html',
    styleUrls: ['app/app.css'],
    directives: [MonyeDirective, DatetimeDirective]
})
export class App {

    money=  12868686868.23;

    date = "2015-12-13";

    datetime = "2015-12-13T20:00:00";

    constructor() {

        numeral.language('fr', {
            delimiters: {
                thousands: ' ',
                decimal: ','
            },
            abbreviations: {
                thousand: 'k',
                million: 'm',
                billion: 'b',
                trillion: 't'
            },
            ordinal : function (number) {
                return number === 1 ? 'er' : 'ème';
            },
            currency: {
                symbol: '€'
            }
        });

        // switch between languages
        numeral.language('fr');

    }
}



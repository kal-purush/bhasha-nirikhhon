import {Component} from 'angular2/core';
import {Proposition} from './proposition';

@Component({
    selector: 'propositions',
    templateUrl: 'app/propositions.component.html'
})

export class PropositionsComponent  {
    propositions: Proposition[];
    propositions= PROPOSITIONS;

}

var PROPOSITIONS: Proposition[] =  [
    {"time": "11 am", "place": "BonChon", "name":"Torg"},
    {"time": "10:30 am", "place": "Kanin Club", "name":"Shell"},
    {"time": "11:00 am", "place": "Yellow Cab", "name":"Jobeth"},
    {"time": "12 noon", "place": "Ramen Nagi", "name":"Gino"}
];
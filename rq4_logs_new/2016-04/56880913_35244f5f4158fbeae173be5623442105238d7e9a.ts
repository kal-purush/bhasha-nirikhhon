/// <reference path="../../node_modules/angular2/core.d.ts" />
/// <reference path="../../typings/browser.d.ts" />

import {Component, ViewEncapsulation} from 'angular2/core';
import {MessageHistoryComponent} from '../message-history/message-history.component';



@Component({
    selector: 'crmin',
    templateUrl: './app/app/app.component.html',
    directives: [MessageHistoryComponent]
    //encapsulation: ViewEncapsulation.Native

})

export class AppComponent {

    title = 'CRMin';

    //selectedPerson: Person;
    //onSelect(person: Person) {
    //    this.selectedPerson = person;
    //}

    //public people = people;
};
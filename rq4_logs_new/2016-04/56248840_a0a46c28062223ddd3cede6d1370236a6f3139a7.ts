import {Component, OnInit} from 'angular2/core';
import {CORE_DIRECTIVES} from 'angular2/common';
import {PersonService} from './person.service';
import {Configuration} from './configuration'
import {Person} from './person.interface';

@Component({
    selector: 'person-detail',
    templateUrl: 'app/person-detail.component.html',
    directives: [CORE_DIRECTIVES],
    providers: [PersonService, Configuration]
})

export class PersonDetailComponent implements OnInit {

    constructor(private _http: PersonService) { }

    selectedPerson: string;
    errorMessage: string;

    ngOnInit() {
        this.getOnePerson();
    }

    private getOnePerson() {
        this._http.getOnePerson()
            .subscribe(
            data => (this.selectedPerson = data.name),
            error => this.errorMessage = <any>error,
            () => console.log("Finished retrieving one person.")
            );
    }
}
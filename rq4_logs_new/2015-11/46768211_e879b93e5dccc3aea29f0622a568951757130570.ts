/// <reference path="../../../typings/angular2-meteor.d.ts" />
/// <reference path="../../../typings/bootstrap/bootstrap.d.ts" />
import {Component, View, bootstrap, provide} from 'angular2/angular2';
import {RouterLink} from 'angular2/router';

@Component({
    selector: 'top-menu'
})
@View({
    templateUrl: 'client/custom/top-menu/top-menu.html',
    directives: [RouterLink]
})
export class TopMenu {
    constructor() {
        console.log("TopMenu component start");
    }

    addNewProjectForm () {
        console.log("addNewProjectForm");
    }
}
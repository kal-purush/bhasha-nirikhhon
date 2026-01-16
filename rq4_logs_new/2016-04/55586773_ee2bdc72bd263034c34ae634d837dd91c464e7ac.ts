/**
 * Created by mvincent on 06/01/2016.
 */
import {Component, View} from 'angular2/core';
import {FORM_DIRECTIVES, CORE_DIRECTIVES} from 'angular2/common';
import {ROUTER_DIRECTIVES, RouteParams, RouteConfig, Router} from "angular2/router";
import {} from "angular2/router";

@Component({
    selector: 'customer',
    properties: ['id'],
    directives: [ROUTER_DIRECTIVES]
})
@View({
    templateUrl: "./templates/customer.html",
    directives: [FORM_DIRECTIVES, CORE_DIRECTIVES]
})
export class CustomerComponent {
    id: string;

    constructor(params: RouteParams) {
        this.id = params.get('id');
    }

}
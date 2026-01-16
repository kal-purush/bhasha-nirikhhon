/**
 * Created by mvincent on 06/01/2016.
 */

/*
import {Component, View} from 'angular2/core';
import {FORM_DIRECTIVES, CORE_DIRECTIVES} from 'angular2/common';
import {ROUTER_DIRECTIVES, RouteParams, RouteConfig, Router} from "angular2/router";
import {CustomerService} from "../services/CustomerService";
import {Customer} from '../models';


@Component({
    properties: ['content'],
    providers: [ROUTER_DIRECTIVES, AccessCodeService, FORM_DIRECTIVES, CORE_DIRECTIVES]
})

@View({
    templateUrl: './templates/_accessCode.html',
    directives: [CORE_DIRECTIVES]
})
export class CustomerComponent {
    customer: Customer = new Customer();

    constructor(params: RouteParams, customerService: CustomerService) {
        console.log("Customer Component");
        this.customer.name="";

        customerService.getById(params.get('id'))
            .subscribe((response:Customer)=> {
                this.customer.name = response.name;
                console.log(this.customer);
            });

    }

}
*/
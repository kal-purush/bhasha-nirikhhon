import { Component } from 'angular2/core';
import { RouteConfig, ROUTER_DIRECTIVES } from 'angular2/router';

import { HomePage } from './home.component';
import { AddProducts } from './product.component';
import { AddLocation } from './location.component';
import { AddCustomer } from './customer.component';
import { AddProject } from './project.component';
import { Search } from './search.component';
import { Defective } from './rma.component';

@Component({
    selector: 'main-window',
    template:   `
                    <nav class="navbar navbar-default">
                        <div class="container-fluid">
                            <!-- Brand and toggle get grouped for better mobile display -->
                            <div class="navbar-header">
                                <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#bs-example-navbar-collapse-1" aria-expanded="false">
                                    <span class="sr-only">Toggle navigation</span>
                                    <span class="icon-bar"></span>
                                    <span class="icon-bar"></span>
                                    <span class="icon-bar"></span>
                                </button>
                                <a class="navbar-brand" href="#">{{ brand }}</a>
                            </div>
                               
                            <!-- Collect the nav links, forms, and other content for toggling -->
                            <div class="collapse navbar-collapse" id="bs-example-navbar-collapse-1">
                                <ul class="nav navbar-nav">
                                    <li><a [routerLink]="['HomePage']">Home</a></li>
                                    <li><a [routerLink]="['AddProducts']">Add Product</a></li>
                                    <li><a [routerLink]="['AddLocation']">Add Location</a></li>
                                    <li><a [routerLink]="['AddCustomer']">Add Customer</a></li>
                                    <li><a [routerLink]="['AddProject']">Add Project</a></li>
                                    <li><a [routerLink]="['Search']">Search</a></li>
                                    <li><a [routerLink]="['Defective']">RMA List</a></li>
                                </ul>
                            </div><!-- /.navbar-collapse -->
                        </div>
                    </nav>
                    <router-outlet></router-outlet> 
                `,
    directives: [ROUTER_DIRECTIVES]
})
@RouteConfig([
    { path: '/home', name: 'HomePage', component: HomePage, useAsDefault: true },
    { path:'/products', name: 'AddProducts', component: AddProducts },
    { path:'/location', name: 'AddLocation', component: AddLocation },
    { path:'/customer', name: 'AddCustomer', component: AddCustomer },
    { path:'/project', name: 'AddProject', component: AddProject },
    { path:'/search', name: 'Search', component: Search },
    { path:'/rma', name: 'Defective', component: Defective }
])
export class MainWindow { 
    public brand = "Asset Management";
}
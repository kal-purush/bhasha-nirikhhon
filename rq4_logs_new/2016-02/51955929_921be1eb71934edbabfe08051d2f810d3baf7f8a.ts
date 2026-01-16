import {Component} from 'angular2/core';
import {RouteConfig, ROUTER_DIRECTIVES, ROUTER_PROVIDERS} from 'angular2/router';
import {DashboardComponent} from "./dashboard.component";

@RouteConfig([
    { path: '/dashboard', name: 'Dashboard', component: DashboardComponent},
])
@Component({
    selector: 'hello-world',
    template: `
      <a [routerLink] = "['Dashboard']">Dashboard</a>
      <router-outlet></router-outlet>
    `,
    directives: [ROUTER_DIRECTIVES],
    providers: [ROUTER_PROVIDERS]
})
export class HelloWorld {
    engineers:Engineer[];

    constructor() {
        this.engineers = [new Engineer("Chris"), new Engineer("Petra"), new Engineer("Ernst8"),
            {
            name: 'Hans',
            ref: "http://www.nu.nl"
        }, new Engineer("Klaasjan"), {name: 'Arjen', ref: "www.nu.nl"}, {
            name: 'Marleen',
            ref: "www.nu.nl"
        }, {name: 'Sjoerd', ref: "www.nu.nl"}, {name: 'Rogier', ref: "www.nu.nl"}];
        this.engineers.push({name: 'Klaasjan @ ' + new Date(), ref: "www.nu.nl"});
        this.engineers.push({name: 'Diederik Refactores', ref: "www.nu.nl"});
    }
}


export class Engineer {
    name:string;
    ref:string;

    constructor(name:string) {
        this.name = name;
        this.ref = "http://www.nu.nl";
    }
}

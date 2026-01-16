/// <reference path="../typings/angular2-meteor.d.ts" />
/// <reference path="../typings/testDemo.d.ts" />

import {Component, View, NgModel, NgFor, provide } from 'angular2/angular2';

import {ROUTER_DIRECTIVES, RouteConfig} from 'angular2/router';

import {ROUTER_PROVIDERS, HashLocationStrategy, LocationStrategy} from 'angular2/router';

import {PartiesCmp} from './party-list';
import {PartiesAdd} from './new-party';

import {Parties} from 'collections/parties';

import {bootstrap} from 'angular2-meteor';


@Component({
    selector: 'app'
})

@View({
    template: '<router-outlet></router-outlet>',
    directives: [ROUTER_DIRECTIVES, PartiesCmp]
})

@RouteConfig([
    {path: '/', as: 'Home', component: PartiesCmp},
    {path: '/new-party', as: 'NewParty', component: PartiesAdd},
    {path: '/edit-party/:partyId', as: 'EditParty', component: PartiesAdd}
])
export class MeteorDemo {
}

bootstrap(MeteorDemo, [
    ROUTER_PROVIDERS, provide(LocationStrategy, {useClass: HashLocationStrategy})
]);
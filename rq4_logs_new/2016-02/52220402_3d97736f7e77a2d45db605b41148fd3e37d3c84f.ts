/**
 * Created by DGeorgiev on 21.2.2016 г..
 */
import {Component} from 'angular2/core';
import {RouteConfig, ROUTER_DIRECTIVES, ROUTER_PROVIDERS} from 'angular2/router';

import {HeroesComponent} from "./heroes.component";
import {HeroService} from "../services/hero.sevice";
import {DashboardComponent} from "./dasboard.component";
import {HeroDetailComponent} from "./hero-detail.component";

@Component({
    selector: 'app',
    templateUrl: 'app/templates/components/app.component.html',
    styleUrls: ['app/styles/app.component.css'],
    directives: [ROUTER_DIRECTIVES],
    providers: [
        ROUTER_PROVIDERS,
        HeroService
    ]
})
@RouteConfig([
    {
        path: '/heroes',
        name: 'Heroes',
        component: HeroesComponent
    },
    {
        path: '/dashboard',
        name: 'Dashboard',
        component: DashboardComponent,
        useAsDefault: true
    },
    {
        path: '/detail/:id',
        name: 'HeroDetail',
        component: HeroDetailComponent
    }
])
export class AppComponent {
    public title:string = 'Tour of Heroes';
}
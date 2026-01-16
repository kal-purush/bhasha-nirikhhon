import {Component} from 'angular2/core'
import {RouteConfig, ROUTER_DIRECTIVES} from 'angular2/router'
import {JokesComponent} from '../components/jokes.component'
import {AboutComponent} from '../components/about.component';

@Component({
    selector: 'my-app',
    template: `
        <h1>Emu Fatty Stack</h1>
        <h3>github/dgroomes</h3>
        <a [routerLink]="['About']">About</a>
        <a [routerLink]="['Jokes']">Jokes</a>
        <router-outlet></router-outlet>
    `,
    directives: [ROUTER_DIRECTIVES]
})
@RouteConfig([
    {name: 'About', path: '/about', component: AboutComponent, useAsDefault: true},
    {name: 'Jokes', path: '/jokes', component: JokesComponent}
])
export class AppComponent {}
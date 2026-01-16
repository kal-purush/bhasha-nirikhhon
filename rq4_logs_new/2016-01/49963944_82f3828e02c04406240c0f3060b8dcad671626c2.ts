import {Component}                 from 'angular2/core';
import {RouteConfig, RouterOutlet,
        ROUTER_DIRECTIVES}         from 'angular2/router';
import {HeroesComponent}           from './hero/heroes.component';
import {HeroDetailComponent}       from './hero/hero-detail/hero-detail.component';
import {DashboardComponent}        from './dashboard/dashboard.component';
import {HeroService}               from './hero/hero.service';

@Component({
  selector: 'my-app',
  template: `
    <h1>{{title}}</h1>
    <a [routerLink]="['Dashboard']">Dashboard</a>
    <a [routerLink]="['Heroes']">Heroes</a>
    <router-outlet></router-outlet>
  `,
  styles: [`
    a {padding: 5px;text-decoration: none;}
    a:visited, a:link {color: #444;}
    a:hover {color: white; background-color: #1171a3;}
    a.router-link-active {color: white; background-color: #52b9e9;}
  `],
  directives: [ROUTER_DIRECTIVES, RouterOutlet],
  providers: [HeroService]
})
@RouteConfig([
  {path: '/',           redirectTo: ['Dashboard'] },
  {path: '/dashboard',  name: 'Dashboard',  component: DashboardComponent, useAsDefault: true},
  {path: '/heroes',     name: 'Heroes',     component: HeroesComponent},
  {path: '/detail/:id', name: 'HeroDetail', component: HeroDetailComponent}
])
export class AppComponent {
  public title = 'Tour of Heroes';
}
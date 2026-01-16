import { Component }       from 'angular2/core';
import { RouteConfig, ROUTER_DIRECTIVES, ROUTER_PROVIDERS } from 'angular2/router';

import {Hero} from './hero';
import { HeroService }     from './hero.service';
import { MenuService }     from './menu.service';
import { DashboardComponent } from './dashboard.component';
import { HeroesComponent } from './heroes.component';
import { HeroDetailComponent } from './hero-detail.component';


@Component({
  selector: 'my-app',
template: `
  <nav class="navbar navbar-default navbar-fixed-top">
      <div class="container">
        <div class="navbar-header">
          <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
            <span class="sr-only">Toggle navigation</span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
          </button>
          <a class="navbar-brand" href="#">Project name</a>
        </div>
        
        <div id="navbar" class="navbar-collapse collapse">
          <ul class="nav navbar-nav">
            <li class="active"><a [routerLink]="['Dashboard']">Dashboard</a></li>
            <li><a [routerLink]="['Dashboard','DashboardDetail']">Dashboard detail</a></li>
            <li><a [routerLink]="['Heroes']">Heroes</a></li>
          </ul>
        </div><!--/.nav-collapse -->
      </div>
    </nav>
    <div class="container-fluid">
      <div class="row">
        <div class="col-sm-3 col-md-2 sidebar">
          <ul class="nav nav-sidebar">         
            <li  *ngFor="#menu of menus"><a [routerLink]="['Dashboard']">{{menu.name}}<span class="sr-only">(current)</span></a></li>
          </ul>
       </div>
       <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
           <router-outlet></router-outlet>
       </div>
    </div>
   </div>
`,

 directives: [ROUTER_DIRECTIVES],
 providers: [
  ROUTER_PROVIDERS,
  HeroService,
  MenuService
 ],
 // styleUrls: ['app/app.component.css'],


})
@RouteConfig([
  {
    path: '/heroes',
    name: 'Heroes',
    component: HeroesComponent
  },
{
  path: '/dashboard/...',
  name: 'Dashboard',
  component: DashboardComponent,

},
{
  path: '/detail/:id',
  name: 'HeroDetail',
  component: HeroDetailComponent
}
])

export class AppComponent {
  title = 'Tour of Heroes';
    menus: Hero[];
   constructor(
    private _menuService: MenuService) { }
  getMenuItems() {
    this._menuService.getMenuItems().then(heroes => this.menus = heroes);
  }
  ngOnInit() {
    this.getMenuItems();
  }
  
}
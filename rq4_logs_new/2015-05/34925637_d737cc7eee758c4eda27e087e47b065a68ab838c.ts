//app.ts
/// <reference path='../typings/aurelia/aurelia.d.ts' />
import {IRouterConfig,Router} from 'aurelia-router';
//
import 'bootstrap';
import 'bootstrap/css/bootstrap.css!';

export class App {
	public router:Router; 
	 configureRouter(config:IRouterConfig, router:Router){
    config.title = 'Aurelia';
    config.map([
      { route: ['','welcome'],  moduleId: './welcome',      nav: true, title:'Welcome' },
      { route: 'flickr',        moduleId: './flickr',       nav: true },
      { route: 'child-router',  moduleId: './child-router', nav: true, title:'Child Router' }
    ]);

    this.router = router;
  }
}
import {Router, RouterConfiguration} from 'aurelia-router'

export class App {
  router: Router;
  routes: any[] = [];
  
  configureRouter(config: RouterConfiguration, router: Router) {  
    config.title = 'Marvelous.Software';

    this.routes.push({ route: ['experimental'], name: 'experimental', moduleId: './experimental/experimental', nav: false, title:'Experimental Features' });
    this.routes.push({ route: ['grid', ''], name: 'grid', moduleId: './grid/grid', nav: true, title:'Grid' });
    this.routes.push({ route: ['query-language'], name: 'query-language', moduleId: './query-language/query-language', nav: true, title:'Query Language' });
    this.routes.push({ route: ['forms'], name: 'forms', moduleId: './forms/forms', nav: true, title:'Forms' });
    config.map(this.routes);

    this.router = router;
  }
}
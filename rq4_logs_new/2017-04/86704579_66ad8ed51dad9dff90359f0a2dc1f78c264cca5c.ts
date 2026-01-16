import { Router, RouterConfiguration } from 'aurelia-router';

export class Configure {

  router: Router;

  configureRouter(config: RouterConfiguration, router: Router) {
    config.map([
      { route: '', redirect: 'profiles' },
      { route: 'profiles', name: 'profiles', moduleId: 'admin/profiles', nav: true, title: 'Profiles' },
      { route: 'boards', name: 'boards', moduleId: 'admin/boards', nav: true, title: 'Boards' },
      { route: 'charts', name: 'charts', moduleId: 'admin/charts', nav: true, title: 'Charts' },
      { route: 'adapters', name: 'adapters', moduleId: 'admin/adapters', nav: true, title: 'Adapters' }
    ]);

    this.router = router;
  }

}
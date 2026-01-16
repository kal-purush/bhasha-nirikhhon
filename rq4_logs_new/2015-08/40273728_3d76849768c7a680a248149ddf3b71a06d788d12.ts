import {Component, View, bootstrap, For, bind, Inject, NgIf} from 'angular2/angular2';
import {routerInjectables, LocationStrategy, HashLocationStrategy, RouteRegistry} from 'angular2/router';
import {RootRouter, Pipeline, RouterLink, RouteConfig, Router, RouterOutlet, Location, RouteParams} from 'angular2/router';
import {Http, httpInjectables} from 'angular2/http';
import {AllComponents} from 'js/route/AllComponents';
import {RouteItem} from 'js/model/RouteItem';
import {Menu} from 'js/route/Menu';
import {NavigationService} from 'js/service/NavigationService';

@Component({
  selector: 'my-app',
})
@View({
	templateUrl: '../html/app.html',
	directives: [RouterLink, RouterOutlet, NgIf, Menu]
})

class TaskAppComponent {
	router: Router;
    location: Location;
    ready: boolean;
    menuItems: Array<RouteItem>;
    constructor( @Inject(Router) router: Router, @Inject(Location) location: Location, 
		@Inject(RouteRegistry) registry: RouteRegistry, @Inject(NavigationService) navigationService: NavigationService) {
        this.router = router;
        this.location = location;
        this.ready = false;
        this.menuItems = [];

        navigationService.loadConfigData().then((data: Array<RouteItem>) => {

			var confData = [];

			data.forEach(item => {

				var confObj = {
					"component": item.component,
					"path": item.path,
					"as": item.as,
				};

				confData.push(confObj);
			});

			// Add route config here
			router.config(confData);

			// Add child routes
			data.forEach(item => {
				var childRouteItems = [];
				var defaultChild = undefined;

				item.childRouteItems.forEach(child => {
					defaultChild = child;

					// Register child routes
					registry.config(item.component, {
						"component": child.component,
						"path": child.path,
						"as": child.as,
					}, false);
				});
				var rlink = item.as;
				var rpath = item.path;
				if (defaultChild) {
					rlink = rlink + '/' + defaultChild.as;
					//rpath = item.path.slice(0, item.path.indexOf('.') - 1);
				}
				this.menuItems.push(new RouteItem(item.component, item.name, rlink, rpath));

			});

			this.ready = true;
		});
    }
    getLinkStyle(path) {
        return this.location.path() === path;
    }
}

bootstrap(TaskAppComponent, [NavigationService, AllComponents, httpInjectables, routerInjectables, bind(LocationStrategy).toClass(HashLocationStrategy)]);

import {bootstrap, Component, provide} from "angular2/angular2";
import {RouteConfig, ROUTER_PROVIDERS, LocationStrategy, HashLocationStrategy} from "angular2/router";
import {Tabs, Tab} from "./components/tabs/tabs"

@Component({
	template: `
		<div> Tab 1 content goes here </div>
	`
})
class Tab1 {}

@Component({
	template: `
		<div> Tab 2 content goes here </div>
	`
})
class Tab2 {}

@Component({
	selector: 'my-app',
	template: `
		<tabs>
			<tab tab-title="Tab 1" path="/Tab1" />
			<tab tab-title="Tab 2" path="/Tab2" />
		</tabs>
	`,
	directives: [Tabs, Tab]
})

@RouteConfig([
	{path:"/", component: Tab1, as:"Tab1"},
	{path:"/Tab1", component: Tab1, as:"Tab1"},
	{path:"/Tab2", component: Tab2, as:"Tab2"}
])

class AppComponent {}

bootstrap(AppComponent, [
	ROUTER_PROVIDERS,
	provide(LocationStrategy, {useClass: HashLocationStrategy})
]);
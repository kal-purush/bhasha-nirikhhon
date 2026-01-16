import {Component, ViewEncapsulation} from "@angular/core";
import { UserService } from "app/users/user.service";

// import {HomeView} from "app/home.view";
// import {SecuredView} from "app/secured.view"
// import {ErrorView} from "app/error.view";

import template from "./app.component.html!text";
import style from "./app.component.css!text";

@Component( {
	selector: "app",
	template: template,
	styles: [ style ],
	encapsulation: ViewEncapsulation.None,
	providers: [  ]  
} )
export class AppComponent {
	constructor() {}
}

export default AppComponent;
import {Component} from 'angular2/core';
import {FORM_DIRECTIVES} from 'angular2/common';
import {Http} from 'angular2/http';

import {Title} from '../../providers/title-service';
import {AuthService} from '../../providers/auth-service';

import {Router} from 'angular2/router';

@Component({
    // The selector is what angular internally uses
    // for `document.querySelectorAll(selector)` in our index.html
    // where, in this case, selector is the string 'app'
    selector: 'login',  // <home></home>
    // We need to tell Angular's Dependency Injection which providers are in our app.
    providers: [
        Title
    ],
    // We need to tell Angular's compiler which directives are in our template.
    // Doing so will allow Angular to attach our behavior to an element
    directives: [
        ...FORM_DIRECTIVES,
    ],
    // We need to tell Angular's compiler which custom pipes are in our template.
    pipes: [],
    // Our list of styles in our component. We may add more to compose many styles together
    styles: [require('./login.css')],
    // Every Angular template is first compiled by the browser before Angular runs it's compiler
    template: require('./login.html')
})
export class Login {
    // TypeScript public modifiers
    private _authService: AuthService;

    constructor(public title: Title, public http: Http,
                private authService: AuthService, private router: Router) {
        this._authService = authService;
    }

    ngOnInit() {
        console.log('Hello Login component');
    }

    login(event, username, password) {
        // This will be called when the user clicks on the Login button
        event.preventDefault();

        if (username !== '' && password !== '') {
            this._authService.logIn();
            this.router.navigate(['Home']);
        }

    }
}
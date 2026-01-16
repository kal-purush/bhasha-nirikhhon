import {Component}              from 'angular2/core';
import {Contacts}               from './contacts.component';
import {ContactComponent}       from './contact.component';
import {ContactDetailComponent} from './contact.detail.component';
import {About}                  from './about.component';
import {ROUTER_DIRECTIVES}      from 'angular2/router';
import {RouteConfig}            from 'angular2/router';

@Component({
    selector: 'my-app',
    styleUrls: ['app/css/app.css'],
    template: `
        <div id="myApp">
            <h3>Component : my-app</h3>
            <header>
                <nav>
                    <a [routerLink]="['Contacts']">Contacts</a>
                    <a [routerLink]="['About']">About</a>
                </nav>
            </header>
            <div>
                <router-outlet></router-outlet>
            </div>
        </div>
    `,
    directives: [ROUTER_DIRECTIVES]
})
@RouteConfig([
    {path:'/contacts', name: 'Contacts', component: Contacts,          useAsDefault: true},
    {path:'/contact/:lastname/:firstname/:age',  name: 'Contact',  component: ContactComponent},
    {path:'/contactDetail',  name: 'ContactDetail',  component: ContactDetailComponent},
    {path:'/about',    name: 'About',    component: About}
])
export class AppComponent { }
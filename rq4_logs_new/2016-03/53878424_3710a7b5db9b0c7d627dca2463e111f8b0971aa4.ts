/**
 * Created by christiancueni on 05/03/16.
 */
import {Component} from 'angular2/core';
import {RouteConfig, ROUTER_DIRECTIVES} from 'angular2/router';
import {TopMenuComponent} from './topmenu.component';
import {AboutComponent} from './about/about.component';
import {BooksComponent} from './books/books.component';

@Component({
    selector: 'book-app',
    directives: [ROUTER_DIRECTIVES, TopMenuComponent],
    templateUrl: '/app/app.component.html'
})

@RouteConfig([
    { // Books child route
        path: '/books/...',
        name: 'Books',
        component: BooksComponent
    },
    {path: '/',   name: 'About', component: AboutComponent, useAsDefault: true}
])

export class AppComponent { }
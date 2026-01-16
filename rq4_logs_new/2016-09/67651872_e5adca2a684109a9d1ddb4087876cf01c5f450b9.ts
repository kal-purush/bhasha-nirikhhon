import { Component} from '@angular/core';
import { RouteConfig , ROUTER_DIRECTIVES} from '@angular/router-deprecated';
/*------------------------ Components ---------------------*/
import {NavigationComponent} from './navigation.component';
import { UsersComponent } from './users/users.component';
import { PostsComponent} from './posts/posts.component';
import { HomeComponent} from './home.component';
import {NewUserComponent } from './users/newuser.component';
import {NotFoundComponent} from './notfound.component';
/*------------------- Routes Definition ------------------*/ 
@RouteConfig([ 
    { path: '/' , name: 'Home' , component: HomeComponent, useAsDefault: true  },
    { path: '/users',  name: 'Users', component: UsersComponent },
    { path:  '/users/new', name:'New', component: NewUserComponent},
    { path: '/posts',  name: 'Posts', component: PostsComponent },
    { path: '/users/:id' , name: 'EditUser', component: NewUserComponent},
    { path: 'notfound' , name:  'NotFound' , component:  NotFoundComponent},
    { path:  '/*other' , name: 'Other', redirectTo: ['Home']}
])
@Component({
    selector: 'my-app',
    template: 
    `<app-navigation></app-navigation>
    <div class="container-fluid">
        <router-outlet></router-outlet>
    </div>`, 
    directives: [ NavigationComponent , ROUTER_DIRECTIVES]
})
export class AppComponent {
    constructor() { }

}
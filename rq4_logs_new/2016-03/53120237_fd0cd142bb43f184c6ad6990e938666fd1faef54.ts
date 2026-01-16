import {Component, OnInit} from "angular2/core";
import { RouteConfig, ROUTER_DIRECTIVES, ROUTER_PROVIDERS } from 'angular2/router';
import {HomeComponent} from "./home.component";
import {ArticleDetailComponent} from "./article-detail.component";
import {Header} from "./header.component";
import {Footer} from "./footer.component";
@Component({
    selector: 'my-app',
    templateUrl: 'app/layout/main.html',
    directives: [ROUTER_DIRECTIVES, Header, Footer],
    providers: [
        ROUTER_PROVIDERS
    ]
})
@RouteConfig([
  {
    path: '/',
    name: 'Home',
    component: HomeComponent,
    useAsDefault: true
  },
  {
    path: '/article/detail/:id',
    name: 'ArticleDetail',
    component: ArticleDetailComponent
  }
])
export class AppComponent {    
    
} 
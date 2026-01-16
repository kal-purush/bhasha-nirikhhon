import {NgClass} from "angular2/common";
import {Component} from "angular2/core";
import {Headers} from "angular2/http";
import {OnInit} from "angular2/core";
import {HTTP_PROVIDERS} from "angular2/http";
import {Http} from "angular2/http";
import {ResultModuleComponent} from "./result-module.component";
import {ResultDataService} from "../services/result-data.service";


@Component({
    selector: 'my-app',
    templateUrl: 'app/components/app.component.html',
    styleUrls: ['app/components/app.component.css'],
    directives: [NgClass, ResultModuleComponent],
    providers: [HTTP_PROVIDERS, ResultDataService],
})


export class AppComponent{
    title = 'Rocket League Rank Progress';
}
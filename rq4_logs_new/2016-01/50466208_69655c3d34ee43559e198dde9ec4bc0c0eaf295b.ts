import {Component, Injectable} from 'angular2/core';
import {bootstrap} from 'angular2/platform/browser';
import {TranslateService, TranslatePipe} from 'ng2-translate/ng2-translate';
import {HTTP_PROVIDERS, Http} from 'angular2/http';

import * as Rx from 'rxjs/Rx';

@Injectable()
@Component({
    selector: 'hello-app',
    template: `
        Normal Expression Copyright: {{version}}
        <br/>
        Expression with Translate Pipe Copyright: "<b>{{ 'FOOTER.COPYRIGHT' | translate:{version: version} }}</b>"
        <br/>
        <button (click) = 'changeVersion()'>change version value (set the value with hardcode value)</button>
        <br/>
        <br/>
        <button (click) = 'getVersion()'>request http to change version value (set the value with http request result)</button>
        <br/>
        Change langage:
        <select (change)="translate.use($event.target.value)">
            <option *ngFor="#lang of languages" [selected]="lang === translate.currentLang">{{lang}}</option>
        </select>
    `,
    pipes: [TranslatePipe]
})
export class HelloApp {
    queryResult: string;
    version: string = '1.0.0';
    languages: Array<string> = ['en', 'an'];
    http: Http;

    constructor(public translate: TranslateService, http: Http) {
        this.http = http;
        
        // not required as "en" is the default
        translate.setDefaultLang('en');

        var userLang = navigator.language.split('-')[0]; // use navigator lang if available
        userLang = /(an|en)/gi.test(userLang) ? userLang : 'en';

        // Use a static file
        translate.useStaticFilesLoader("i18n", ".json");

        // this trigger the use of the userLang language after setting the translations
        translate.use(userLang);
    }
    
    changeVersion () {
        this.version = 'version 2.2.2.2 changed from click';
    }
    
    getVersion () {
        // I got it from http://openweathermap.org/current,
        // if it was expired, you can refresh from this site.
        this.http.request('http://api.openweathermap.org/data/2.5/weather?q=London,uk&appid=44db6a862fba0b067b1930da0d769e98')
        .map((res : any) => {
            this.queryResult = JSON.parse(res._body).coord.lon;
            return Rx.Observable.of(this.queryResult);
        }).subscribe(
            (res : any) => {
                this.version = res.value;
                console.log(this.version);
            }
        );
    }
}

// Instantiate TranslateService in the bootstrap so that we can keep it as a singleton
bootstrap(HelloApp, [TranslateService, HTTP_PROVIDERS]);
///<reference path="../../../../node_modules/angular2/typings/browser.d.ts"/>

import { bootstrap } from 'angular2/platform/browser';
import { Component, AfterViewInit, ViewChild } from 'angular2/core';
@Component({
    selector: 'app',
    template: `
Hello 12345...wow this is cool
  `
})
export class App{
    constructor() {
        console.log('created App');
    }

}

bootstrap(App, []).catch(err => console.error(err));
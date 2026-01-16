/// <reference path="typings/angular2/angular2.d.ts" />
import {Component, View, bootstrap} from 'angular2/angular2';
import {FORM_DIRECTIVES, FormBuilder, ControlGroup} from 'angular2/angular2';

@Component({
    selector: 'my-app',
    viewBindings : [FormBuilder]
})
@View({
    directives: [FORM_DIRECTIVES],
    template: `
        <h1 id="output">My SECOND Angular 2 App</h1>
        <form [ng-form-model]="myForm">
            <input type="text" [ng-form-control]="myForm.controls['sku']"/>
        </form>
    `
})
class AppComponent {
    myForm : ControlGroup;

    constructor(fb:FormBuilder) {
        this.myForm = fb.group({
            "sku" : ['ABC']
        });
    }
}

bootstrap(AppComponent);
import {Component, bootstrap} from 'angular2/angular2';
@Component({
    selector: 'my-app',
    template: '<h1>Hello {{name}}</h1>'
})
class AppComponent {
    name: string;
    constructor() {
        this.name = "Jasmine";
    }
}
bootstrap(AppComponent);
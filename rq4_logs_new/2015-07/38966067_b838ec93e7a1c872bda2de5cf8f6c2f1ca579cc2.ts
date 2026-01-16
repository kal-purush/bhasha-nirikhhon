/// <reference path="typings/angular2/angular2.d.ts" />
import {Component, View, bootstrap} from 'angular2/angular2';

@Component({
    selector: 'display'
})
@View({
    template: `
        <p>My name: {{ myName }}</p>
        <p>Friends:</p>
        <ul>
            <li *ng-for="#name of names">
                {{ name }}
            </li>
        </ul>
    `,
    directives: [NgFor]
})

class DisplayComponent{

    public myName: string;
    public names: Array<string>;

    constructor(){
        this.myName = 'Paolo';
        this.names = ['Paolo', 'Luca', 'Giulia'];
    }
}
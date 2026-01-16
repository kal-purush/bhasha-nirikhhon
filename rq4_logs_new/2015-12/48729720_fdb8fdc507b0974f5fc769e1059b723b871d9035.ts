import {Component, NgFor} from 'angular2/core';

interface Example {
  link: string;
  title: string;
}


@Component({
    selector: 'my-app',
    template: `
        <ul>
            <li *ngFor="#example of examples" >
                <a href="{{example.link}}" target="_blank">{{example.title}}</a>
            </li>
        </ul>
    `
})
export class AppComponent { 
    public examples : Example[] = [{
        link: 'ex01.html',
        title: '5分鐘快速入門範例'
    },{
        link: 'ex03.html',
        title: 'Tutorial:The Hero Editor(Two-Way Binding)'
    },{
        link: 'ex04.html',
        title: 'Tutorial:Master/Detail(ngFor,ngIf,css style)'
    }];
    
    constructor() {
    
    }
}
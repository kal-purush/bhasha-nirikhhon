/**
 * Created by ozgur.tuzgen on 28.12.2015.
 */

interface Hero{
    id: number;
    name: string;
}

import {Component} from 'angular2/core';
@Component({
    selector: 'my-app',
    template: `
    <h1>{{title}}</h1>
    <h2>{{hero.name}} details!</h2>
    <div><label>id: </label>{{hero.id}}</div>
    <div>
      <label>name: </label>
      <div><input [(ngModel)]="hero.name" placeholder="name"></div>
    </div>
    `
})

export class AppComponent{
    title: string = 'Tour of Heroes';
    hero: Hero = {
        id: 1,
        name: 'Windstorm'
    };
}
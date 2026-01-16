import {Component} from 'angular2/core';

interface Hero {
  id: number;
  name: string;
}

@Component({
    selector: 'my-app', // 套用 HTML 元素
    template: `
        <h1>{{title}}</h1>
        <h2>{{hero.name}} details!</h2>
        <div>
            <label>id: </label>{{hero.id}}
        </div>
        <div>
            <label>name: </label>
            <div><input value="{{hero.name}}" placeholder="name2"></div>
            <!-- two-way binding hero.name -->
            <div><input [(ngModel)]="hero.name" placeholder="name"></div>
        </div>
    `
})
export class AppComponent { 
    public title = 'Tour of Heroes';
    public hero: Hero = {
      id: 1,
      name: 'Arick'
    };
}
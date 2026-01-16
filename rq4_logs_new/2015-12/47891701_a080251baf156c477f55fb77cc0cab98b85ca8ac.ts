//https://angular.io/docs/ts/latest/tutorial/toh-pt1.html
class Item {
    id:number;
    name:string;
    tags:array;
}

import {bootstrap, Component, FORM_DIRECTIVES, NgFor} from 'angular2/angular2';

@Component({
    selector: 'my-app',
    directives: [FORM_DIRECTIVES, NgFor],
    template: `
    <h1>{{title}}</h1>
    <div>
        <label>name: </label>{{item.name}}
    <div><input [(ng-model)]="item.name" placeholder="name"></div>
        <label>tags: </label>{{item.tags}}
    <div><input [(ng-model)]="item.tags" placeholder="tags"></div>
    <button (click)="onClick()">add {{ item.name }}</button>
    </div>

    <h2>My Items</h2>
    <ul class="items">
      <li *ng-for="#item of items">
        <span class="badge">{{item.id}}</span> {{item.name}}
      </li>
    </ul>
    `,
    styles: [`
  .items {list-style-type: none; margin-left: 1em; padding: 0; width: 10em;}
  .items li { cursor: pointer; position: relative; left: 0; transition: all 0.2s ease; }
  .items li:hover {color: #369; background-color: #EEE; left: .2em;}
  .items .badge {
    font-size: small;
    color: white;
    padding: 0.1em 0.7em;
    background-color: #369;
    line-height: 1em;
    position: relative;
    left: -1px;
    top: -1px;
  }
  .selected { background-color: #EEE; color: #369; }
  `],
})

class AppComponent {
    public title = 'Awesome Knowledge';
    public item:Item = {
        id: 3,
        name: 'ref...',
        tags: ['...'],
    };
    public items = ITEMS;

    onClick() {
        this.items.push({id: this.items.length + 1, name: this.item.name, tags: this.item.tags});
    }
}

bootstrap(AppComponent);

var ITEMS:Item[] = [
    {
        id: 1,
        name: 'http://www.fullstackradio.com/31',
        tags: ['architecture', 'estimate', 'design'],
    }
];
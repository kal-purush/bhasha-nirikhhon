//Using Angular version 2.0.0-alpha.37

import {bootstrap, Component, NgFor, View} from 'angular2/angular2';
import {HTTP_BINDINGS, Http} from 'angular2/http';

@Component({
  selector: 'app',
  bindings: [HTTP_BINDINGS]
})
@View({
  template: `
    <div>
      <h1>Free Wifi Here</h1>
      <ul>
        <li *ng-for="#place of places">
          <b><a href='details.html?name={{place.Name}}'>{{place.Name}}</a></b>
        </li>
      </ul>
    </div>
  `,
  directives: [NgFor]
})
export class App {
  places: Object[];
  constructor(http:Http) {
    http.get('http://www.findfreewifi.co.za/publicjson/getcities').toRx().subscribe(res => {
      this.places = res.json().data;
    });
  }
  active:boolean = false;
  toggleActiveState() {
    this.active = !this.active;
  }
}

bootstrap(App)
  .catch(err => console.error(err));
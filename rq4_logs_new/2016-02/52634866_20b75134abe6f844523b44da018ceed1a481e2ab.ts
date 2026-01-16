import {Component} from "angular2/core";
import {Link} from "./link";
import {LinkData} from "./link-data";
import {LinkDetailComponent} from "./link-detail.component";

@Component({
    selector: "Home",
    template: `
    <div class="container">
    <div class="list col-sm-6">
        <h1>{{title}}</h1>
        <ul class="links">
          <li *ngFor="#link of links"
            [class.selected]="link === selectedLink"
            (click)="onSelect(link)"
            class="link">
            <span class="badge">{{link.id}}</span> {{link.name}}
          </li>
        </ul>
    </div>
    <div class="detail col-sm-6">
        <my-link-detail [link]="selectedLink"></my-link-detail>
    </div>
    </div>

  `,
    styles: [`
    .selected {
      background-color: #CFD8DC !important;
      color: white;
    }
    .links {
      margin: 0 0 2em 0;
      list-style-type: none;
      padding: 0;
    }
    .links li {
      cursor: pointer;
      position: relative;
      left: 0;
      background-color: #EEE;
      margin: .5em;
      border-radius: 4px;
    }
    .links li.selected:hover {
      color: white;
    }
    .links li:hover {
      color: #607D8B;
      background-color: #EEE;
      left: .1em;
    }
    .links .text {
      position: relative;
      top: -3px;
    }
    .links .badge {
      display: inline-block;
      font-size: small;
      color: white;
      padding: 0.8em 0.7em 0em 0.7em;
      background-color: #607D8B;
      height: 2.0em;
      margin-right: .8em;
      border-radius: 4px 0px 0px 4px;
    }
    .detail{
      position: fixed;
      right: 5px;
    }
  `],
    directives: [LinkDetailComponent]
})

export class HomeComponent {
    public title = "Links";
    public links = new LinkData().links;
    public selectedLink:Link;

    onSelect(link:Link) {
        this.selectedLink = link;
    }
}

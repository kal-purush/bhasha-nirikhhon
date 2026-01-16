/**
 * Created by Bret on 1/31/2016.
 */
import {Component} from "angular2/core";
import {Link} from "./link";

@Component({
    inputs: ["link"],
    selector: "my-link-detail",
    template: `
    <div *ngIf="link">
      <h2>{{link.name}}</h2>
      <div><label>id: </label>{{link.id}}</div>
      <div>
        <label>url: </label>
        <input [(ngModel)]="link.url" placeholder="url"/>
      </div>
      <div>
        <label>link: </label>
        <a [href]="link.url">{{link.name}}</a>
      </div>
    </div>
  `,
})
export class LinkDetailComponent {
    public link:Link;
}
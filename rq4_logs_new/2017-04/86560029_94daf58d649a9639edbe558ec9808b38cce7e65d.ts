import { Component, Input } from '@angular/core';
import { IPoem } from "../../models/IPoem";
import { InAppBrowser } from "@ionic-native/in-app-browser";

/*
  Generated class for the PoemDetail component.

  See https://angular.io/docs/ts/latest/api/core/index/ComponentMetadata-class.html
  for more info on Angular 2 Components.
*/
@Component({
  selector: 'poem-detail',
  templateUrl: 'poem-detail.html'
})
export class PoemDetailComponent {

  @Input() poem: IPoem  ;
  browser:any;

  constructor(private iab: InAppBrowser) {
    console.log('Hello PoemDetail Component');
    
  }

}
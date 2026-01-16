import { Component } from '@angular/core';
import { ViewController} from 'ionic-angular';

@Component({
  template: `
    <ion-list>
      <button ion-item *ngFor="let item of items;" (click)="close()">{{ item.title }}</button>
    </ion-list>
  `
})
export class PopoverListComponent {

/**
 * @description list items to be displayed on popover
 */
  public items:any;

/**
 * @constructor
 * @param viewCtrl ViewController
 */
  constructor(public viewCtrl: ViewController) {
    this.items = viewCtrl.data.items;
  }

  close() {
    this.viewCtrl.dismiss();
  }
}
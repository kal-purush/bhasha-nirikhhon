import { Component } from '@angular/core';
import { ViewController } from 'ionic-angular';

@Component({
  template: `
    <form name="status" [formGroup]="byStatus">
      <ion-list radio-group [(ngModel)]="autoManufacturers">
        <ion-item *ngFor="let a of status">
          <ion-label>{{a.title}}</ion-label>
          <ion-radio [(ngModel)]="aa" value="{{a.value}}"></ion-radio>
        </ion-item>
      </ion-list>
    </form>
  `
})

export class PopoverPage {

  constructor(public viewCtrl: ViewController) {}

  status = [{
    title: 'Closed',
    value: '1'
  }, {
    title: 'Satisfied',
    value: '2'
  }];

  autoManufacturers = "2";

  close() {
    this.viewCtrl.dismiss();
  }

}
import { Component, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs';
import { MsgBetweenService } from '../msg-between.service';

@Component({
  selector: 'app-msg-child1',
  template: `
  <p>
    msg-between working, child 1: shared through <b>Service</b>
    Showing message as:
    <br>
    {{ jsonMsg }}
  </p>
  `
})
export class MsgChild1Component implements OnDestroy {
  msgSubscription: Subscription;
  jsonMsg = '';

  constructor(private msgBetweenService: MsgBetweenService) {
    this.msgSubscription = this.msgBetweenService.getMessage()
      .subscribe((passedInData: { [key: string]: any | Array<any> }) => {
        this.jsonMsg = JSON.stringify(passedInData);
        this.onExport(passedInData.msgData);
      });
  }

  onExport(msgData: { [key: string]: any | Array<any> }): void {
    console.log('jsonMsg=', this.jsonMsg);
  }

  ngOnDestroy(): void {
    this.msgSubscription.unsubscribe();
  }
}
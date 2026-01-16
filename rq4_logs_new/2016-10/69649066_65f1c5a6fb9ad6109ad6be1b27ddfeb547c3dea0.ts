import {Http, Response } from '@angular/http';
import { Component, OnInit} from '@angular/core';
import { HttpRequest } from './wikisearch';
import 'rxjs/add/operator/toPromise';

import { InvoiceNotificationThresholdWsVo } from './ws.vo.invoiceNotificationThreshold';
import { InvoiceNotificationThresholdWsPostVo } from './ws.post.vo.invoiceNotificationThreshold'
import { InvoiceNotificationThresholdWsPostRespVo} from './ws.post.resp.vo.invoiceNotificationThreshold'

@Component({
  selector: 'my-app',
  template:
  `
     <div *ngIf="invoiceNotificationThresholdWsVo">
      <h1>angular2 http get sample</h1>
      <button (click)="callApi()">callApi</button> 
      <input [(ngModel)]="invoiceNotificationThresholdWsVo.hintThreshold">
      <h1> {{invoiceNotificationThresholdWsVo.created}}</h1>
     </div>
     <hr>
     <div *ngIf="invoiceNotificationThresholdWsVo">
      <h1>angular2 http post sample</h1>
      <input [(ngModel)]="invoiceNotificationThresholdWsPostVo.type" placeholder="type">
      <input [(ngModel)]="invoiceNotificationThresholdWsPostVo.thresholdAmount" placeholder="thresholdAmount">
      <input [(ngModel)]="invoiceNotificationThresholdWsPostVo.srcProject" placeholder="srcProject">
      <button (click)="callPostApi()">callPostApi</button>
      <hr>
        <div *ngIf="invoiceNotificationThresholdWsPostRespVo && invoiceNotificationThresholdWsPostRespVo.returnStatus && invoiceNotificationThresholdWsPostRespVo.returnStatus!=='success' ">  
        
         <h1> error msg {{invoiceNotificationThresholdWsPostRespVo.returnStatus}}</h1>
        </div>  
     </div>
     
   
  
  `
})
export class AppComponent implements OnInit {

  invoiceNotificationThresholdWsVo: InvoiceNotificationThresholdWsVo;
  invoiceNotificationThresholdWsPostVo: InvoiceNotificationThresholdWsPostVo
  invoiceNotificationThresholdWsPostRespVo: InvoiceNotificationThresholdWsPostRespVo


  getApiString = "wefw";

  constructor(
    private searchWiki: HttpRequest) {

  }
  ngOnInit(): void {
    this.invoiceNotificationThresholdWsPostVo = new InvoiceNotificationThresholdWsPostVo("", "", "");
    this.callApi();
  }

  callApi() {

    this.searchWiki.getDataViaPromise("ji", 1).then(invoiceNotificationThresholdWsVo => {
      this.invoiceNotificationThresholdWsVo = invoiceNotificationThresholdWsVo[0];
    }

    );

    //will pass parameter to then block 
  }

  callPostApi() {

    this.searchWiki.postDataViaPromise(this.invoiceNotificationThresholdWsPostVo).then(resp => this.invoiceNotificationThresholdWsPostRespVo = resp[0]).then(invoiceNotificationThresholdWsVo => this.searchWiki.getDataViaPromise("ji", 1).then(invoiceNotificationThresholdWsVo => this.invoiceNotificationThresholdWsVo = invoiceNotificationThresholdWsVo[0]));

    //will pass parameter to then block 
  }

  private extractData(res: any[]) {
    let body = res;
    console.log(body);
    //console.log(body[0]['id']);

    return body || {};
    //return body.data || {};
  }

}
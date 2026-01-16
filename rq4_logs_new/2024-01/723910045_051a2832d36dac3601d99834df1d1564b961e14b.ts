/*
 * @Author: zhangxuefeng
 * @Date: 2024-01-15 14:09:16
 * @LastEditors: zhangxuefeng
 * @LastEditTime: 2024-01-16 14:50:02
 * @Description:
 */
import { Injectable } from '@angular/core';
import {
  ActivatedRoute,
  ActivatedRouteSnapshot,
  Params,
  Router,
  UrlSegment
} from '@angular/router';
import { BehaviorSubject, Observable, Subject, Subscription, interval } from 'rxjs';

import _ from 'lodash';
import { NzModalService } from 'ng-zorro-antd/modal';

@Injectable({
  providedIn: 'root'
})
export class SocketService {
  public MetaArray$ = new BehaviorSubject<string[]>([]);
  private metaArray: string[] = [];
  constructor(
    private router: Router,
    private activatedRoute: ActivatedRoute,
    private modal: NzModalService
  ) {
    this.messageSubject = new Subject();
    console.log('Start heartbeat detection');
    // Perform heartbeat detection as soon as you enter the program to avoid interruption of the connection at the beginning and no subsequent reconnection.
    this.heartCheckStart();
    this.calcRunTime();
  }

  messageSubject; // Subject object, used to send events
  private url!: string; // Default requested url
  private webSocket!: WebSocket; // Websocket object
  connectSuccess = false; // websocket connection successful
  period = 60 * 1000 * 10; // Check every 10 minutes
  serverTimeoutSubscription!:Subscription; // Periodically detect connection objects
  reconnectFlag = false; // Reconnection
  reconnectPeriod = 5 * 1000; // If reconnection fails, reconnect once every 5 seconds.
  reconnectSubscription!:Subscription; // Reconnect subscription object
  runTimeSubscription: any; // Record running connection subscription
  runTimePeriod = 60 * 10000; // Record running connection time


  sendMessage(message: string) {
    this.webSocket.send(message);
  }

  connect(url: string) {
    if (!!url) {
      this.url = url;
    }
    // Create websocket object
    this.createWebSocket();
  }


  createWebSocket() {
    // If no connection has been established, establish the connection and add time monitoring
    this.webSocket = new WebSocket(this.url);
    // Connection established successfully
    this.webSocket.onopen = (e) => this.onOpen(e);
    // message received
    this.webSocket.onmessage = (e) => this.onMessage(e);
    // connection closed
    this.webSocket.onclose = (e) => this.onClose(e);
    // abnormal
    this.webSocket.onerror = (e) => this.onError(e);
  }


  onOpen(e:any) {
    console.log('websocket connected');
    // Set up connection successfully
    this.connectSuccess = true;
    // If it is reconnecting
    if (this.reconnectFlag) {
      // 1.Stop reconnecting
      this.stopReconnect();
      // 2.Restart heartbeat
      this.heartCheckStart();
      // 3.Restart calculation of running time
      this.calcRunTime();
    }
  }


  onMessage(event:any) {
    console.log('message received', event.data);
    // Publish received messages
    const message = JSON.parse(event.data);
    console.log('Message received time', new Date().getTime());
    this.messageSubject.next(message);
  }


  private onClose(e:any) {
    console.log('connection closed', e);
    this.connectSuccess = false;
    this.webSocket.close();
    // Start reconnecting when closed
    this.reconnect();
    this.stopRunTime();
    // throw new Error('webSocket connection closed:)');
  }


  private onError(e:any) {
    // When an exception occurs, on close will always be entered, so only one reconnection action is performed on close.
    console.log('Connection abnormality', e);
    this.connectSuccess = false;
    // throw new Error('webSocket connection error:)');
  }


  reconnect() {
    // If it has been reconnected, return directly to avoid repeated connections.
    if (this.connectSuccess) {
      this.stopReconnect();
      console.log('The connection has been successful, stop reconnecting');
      return;
    }
    // If the connection is in progress, return directly to avoid multiple polling events.
    if (this.reconnectFlag) {
      console.log('Reconnecting, return directly');
      return;
    }
    // Start reconnecting
    this.reconnectFlag = true;
    // If the connection fails, reconnect regularly.
    this.reconnectSubscription = interval(this.reconnectPeriod).subscribe(
      async (val) => {
        console.log(`Reconnection:${val}Second-rate`);
        const url = this.url;
        // reconnect
        this.connect(url);
      }
    );
  }


  stopReconnect() {
    // The connection flag is set to false
    this.reconnectFlag = false;
    // unsubscribe
    if (
      typeof this.reconnectSubscription !== 'undefined' &&
      this.reconnectSubscription != null
    ) {
      this.reconnectSubscription.unsubscribe();
    }
  }


  heartCheckStart() {
    this.serverTimeoutSubscription = interval(this.period).subscribe((val) => {
      // Stay connected and reset
      if (this.webSocket != null && this.webSocket.readyState === 1) {
        console.log(val, 'Connection status, send messages to stay connected');
      } else {
        this.heartCheckStop();
        this.reconnect();
        console.log('The connection has been disconnected, reconnect');
      }
    });
  }


  heartCheckStop() {

    if (
      typeof this.serverTimeoutSubscription !== 'undefined' &&
      this.serverTimeoutSubscription != null
    ) {
      this.serverTimeoutSubscription.unsubscribe();
    }
  }


  calcRunTime() {
    this.runTimeSubscription = interval(this.runTimePeriod).subscribe(
      (period) => {
        console.log('operation hours', `${period}minute`);
      }
    );
  }

  stopRunTime() {
    if (
      typeof this.runTimeSubscription !== 'undefined' &&
      this.runTimeSubscription !== null
    ) {
      this.runTimeSubscription.unsubscribe();
    }
  }
}
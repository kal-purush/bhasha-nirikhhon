import { Subscription } from 'rxjs';
import { AdminoApiService } from './api.service';
import { Injectable } from '@angular/core';
import { AdminoActionService } from './action.service';
import { BackendResponse } from '../interfaces';

@Injectable({
  providedIn: 'root'
})
export class AdminoPingService {

  pingFailed;

  pingTimeout;
  pingSub: Subscription;

  constructor(private api: AdminoApiService, private as: AdminoActionService) {

  }

  init() {
    this.startTimeout();
    this.as.pingFrequency.subscribe((params) => {
      this.startTimeout();
    });
  }

  startTimeout() {
    if (this.as.pingFrequency.value <= 0) {
      this.clearPing();
      return;
    }
    this.pingTimeout = setTimeout(() => {
      this.pingSub = this.api.ping(this.as.activeScreenId).subscribe((response: BackendResponse) => {
        this.as.handleResponse(response);
        this.startTimeout();
      }, (err) => {
        this.startTimeout();
      });
    }, this.as.pingFrequency.value);
  }
  clearPing() {
    if (this.pingTimeout) {
      clearTimeout(this.pingTimeout);
    }
    if (this.pingSub) {
      this.pingSub.unsubscribe();
    }
  }

}
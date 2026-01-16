import { Injectable } from '@angular/core';
import { SwUpdate } from '@angular/service-worker';
import { BehaviorSubject, Observable, Subject } from 'rxjs';

@Injectable({
  providedIn: 'root',
})
export class AppUpdateService {
  private updateAvailable: BehaviorSubject<boolean> = new BehaviorSubject<boolean>(false);

  updateAvaliable$: Observable<boolean> = this.updateAvailable.asObservable();

  constructor(private readonly updates: SwUpdate) {
    this.updates.versionUpdates.subscribe(() => {
      this.updateAvailable.next(true);
    });
  }

  doAppUpdate() {
    this.updates.activateUpdate().then(() => document.location.reload());
  }
}
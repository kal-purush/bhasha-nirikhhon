import { Injectable } from '@angular/core';
import * as _ from 'lodash';
import { BehaviorSubject } from 'rxjs';
import { textChangeRangeIsUnchanged } from 'typescript';

@Injectable({
  providedIn: 'root'
})
export class AppStateService {

  private _footerItems = new BehaviorSubject([]);
  footerItems$ = this._footerItems.asObservable();

  constructor() { }

  addToFooter(footerItem: { content: string }) {
    const items = this._footerItems.getValue();
    this._footerItems.next([...items, footerItem]);
  }

  removeFromFooter(footerItem: { content: string }) {
    const items = this._footerItems.getValue();
    this._footerItems.next(_.without(items, footerItem));
  }
}
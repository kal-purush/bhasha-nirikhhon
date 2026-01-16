import { OnInit, Input } from '@angular/core';

import 'rxjs/add/operator/let';
import { Observable } from 'rxjs/Observable';
import { Component, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { Store } from '@ngrx/store';

import * as fromRoot from '../reducers';
import * as goodsActions from '../actions/good';

import { Good } from '../models/good';

@Component({
  selector: 'bc-good-create',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <bc-goods-detail
      [good]="good"
      (save)="onCreate($event)"
    ></bc-goods-detail>
  `
})
export class GoodCreateContainerComponent implements OnInit {
  good: Good;

  ngOnInit() {
    console.log('on init');
  }

  constructor(private store: Store<fromRoot.State>) {
    console.log('constructor');
    this.good = {
      id: Math.random(),
      name: ''
    };
  }

  onCreate(good: Good) {
    console.log('Item created: ', good);
    this.store.dispatch(new goodsActions.CreateGoodAction(good));
  }
}
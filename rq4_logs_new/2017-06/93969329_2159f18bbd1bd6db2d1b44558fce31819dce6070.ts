import 'rxjs/add/operator/let';
import { Observable } from 'rxjs/Observable';
import { Component, ChangeDetectionStrategy } from '@angular/core';
import { Store } from '@ngrx/store';

import * as fromRoot from '../reducers';
import * as layout from '../actions/layout';


@Component({
  selector: 'bc-goods-container',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <bc-goods-list></bc-goods-list>
    <bc-goods-detail></bc-goods-detail>
  `
})
export class GoodsContainerComponent {}
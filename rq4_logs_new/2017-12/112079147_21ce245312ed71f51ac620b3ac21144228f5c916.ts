import {Component, OnInit} from '@angular/core';
import {FrontendSelectorsIds} from '../../globals/frontend-selectors-ids';
import {BaseComponent} from '../base-component/base.component';
import {IBaseAction} from '../../models/base-action.model';

@Component({
  selector: FrontendSelectorsIds.BaseAction,
  templateUrl: './base-action.component.html'
})
export class BaseActionComponent<TActionModel extends IBaseAction> extends BaseComponent<TActionModel> implements OnInit {

  constructor() {
    super();
  }

  ngOnInit() {
  }

}
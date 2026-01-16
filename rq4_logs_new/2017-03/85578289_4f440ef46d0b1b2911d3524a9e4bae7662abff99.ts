import { Component, OnInit, Input, OnDestroy } from '@angular/core';
import { FormGroup, Validators } from '@angular/forms';

import { Subscription } from 'rxjs'
import * as moment from 'moment'
import { FormField } from '../form-field.model'

@Component({
  selector: 'tw-treeview-txt',
  template: `
  <div [formGroup]='group'>
   <leo-dropdown-treeview id="treeview_{{field.id}}" [config]="selectConfig" [items]="field.options" (selectedChange)="updateModel($event)"></leo-dropdown-treeview>
    <input [formControlName]='field.id' name='{{field.id}}' type='hidden' class='form-control' [(ngModel)]="request[field.id]"/>
</div>
  `
})
export class TreeViewTxtComponent {

  @Input() group: FormGroup
  @Input() field: FormField
  @Input() request: any

  private selectConfig: any = {
    isShowAllCheckBox: true,
    isShowFilter: true,
    isShowCollapseExpand: false
  }

  private updateModel(values?: any[]) {
    if (values)
      this.field.setValue(values.join(','))
    else
      this.field.setValue('')
  }
}
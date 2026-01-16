import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router'
import { FormBuilder, FormGroup, Validators, FormArray } from '@angular/forms';
import { Location } from '@angular/common'
import { tap, filter } from 'rxjs/operators'
import { Store } from '@ngrx/store';
import { State } from '../app.reducers'

@Component({
  selector: 'app-group-edit',
  templateUrl: './group.edit.component.html',
  styles: []
})
export class GroupEditComponent implements OnInit {
  form: FormGroup;
  constructor(private store: Store<State>, private route: ActivatedRoute, private location: Location, private fb: FormBuilder) {}

  ngOnInit() {
    this.form = this.fb.group({
      id: [],
      name: ['', Validators.required],
      hidden: [false],
      inbalance: [true],
      accounts: this.fb.array([])
    });
    this.store.select('groups', 'selected').pipe(filter(g => g != null)).forEach(g => {
      this.form.patchValue(g);
      let accounts = this.fb.array([]);
      this.form.setControl('accounts', accounts);
      g.accounts.forEach(a => accounts.push(this.fb.group({id: a.id, start_balance: a.start_balance, currency: a.currency, deleted: a.deleted})));
    });
    this.route.params.forEach(p => this.store.dispatch({ type: '[group] query id', payload: p['id']}));
  }

  onSubmit({ value, valid }) {
    this.store.dispatch({type: '[group] save', payload: value});
  }

  canDelete(): boolean {
    let accounts = this.form.get('accounts') as FormArray;
    return accounts.controls.filter(a => !a.get('deleted').value).length > 1;
  }

  delete(item) {
    item.get('deleted').setValue(true);
  }

  cancel() {
      this.location.back();
  }
}
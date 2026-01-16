import { Component, OnInit } from '@angular/core';
import { Observable} from 'rxjs';
import { Store } from '@ngrx/store';
import { State } from '../app.reducers'
import { Account } from '../models/account';

@Component({
  selector: 'app-account-edit',
  templateUrl: '/account.edit.component.html',
  styles: []
})
export class AccountEditComponent implements OnInit {
  account$: Observable<Account>;
  constructor(private store: Store<State>) {}

  ngOnInit() {
    this.account$ = this.store.select('accounts', 'selected');
  }
}
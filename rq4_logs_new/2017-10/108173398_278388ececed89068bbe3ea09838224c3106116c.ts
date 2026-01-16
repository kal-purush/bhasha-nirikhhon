import { Component, OnDestroy, OnInit } from '@angular/core';
import { Store } from '@ngrx/store';
import { AppState } from '../../../store/reducers';
import { Observable } from "rxjs/Observable";
import { Subscription } from 'rxjs/Subscription';
import { IUser } from '../../../interfaces';

@Component({
  selector: 'utility-spending',
  templateUrl: 'utility-spending.html'
})
export class UtilitySpendingComponent implements OnDestroy, OnInit {

  private _users: IUser[] = [];
  private _subscriptions: Subscription[] = [];

  constructor(
    private _store: Store<AppState>
  ) {}

  ngOnInit() {
    this._subscriptions.push(this._subscribeToUserDataChange());
  }

  ngOnDestroy() {
    for (const subscription of this._subscriptions) {
      subscription.unsubscribe();
    }
  }

  private _subscribeToUserDataChange(): Subscription {
    return this._store
      .select(state => state.users)
      .subscribe((users: IUser[]) => {
        this._users = users;
      });
  }

}
import { Component, OnInit } from '@angular/core';
import { select, Store } from '@ngrx/store';
import { Observable } from 'rxjs';
import User from '../../../../shared/model/user.model';
import { LoadUser } from '../../../user-profile-page/state/user.action';
import { selectUserState, StateWithUser } from '../../../user-profile-page/state/user.reducer';

@Component({
  selector: 'smart-user-profile-page',
  template: `
    <app-user-profile-page
      [user]="user$ | async"
      (loadUser)="loadAnotherUser()"
    ></app-user-profile-page>

    <br>
    <span *ngIf="isLoading$ | async" class="alert alert-info mt-5">Loading user...</span>
    <span *ngIf="hasError$ | async" class="alert alert-danger mt-5">Could not load user...</span>
  `,
  styleUrls: ['./smart-user-profile-page.scss']
})
export class SmartUserProfilePageComponent implements OnInit {
  user$: Observable<User>;
  hasError$: Observable<boolean>;
  isLoading$: Observable<boolean>;

  constructor(private store: Store<StateWithUser>) { }

  ngOnInit(): void {
    this.user$ = this.store.select(selectUserState).pipe(select(userState => userState.data));
    this.hasError$ = this.store.select(selectUserState).pipe(select(userState => userState.error));
    this.isLoading$ = this.store.select(selectUserState).pipe(select(userState => userState.loading));
  }

  loadAnotherUser(){
    this.store.dispatch(new LoadUser());
  }

}
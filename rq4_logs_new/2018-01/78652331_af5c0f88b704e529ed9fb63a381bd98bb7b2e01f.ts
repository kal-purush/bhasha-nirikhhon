import { Component, Input, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs/Subscription';

import { ToastOptions, ToastyService } from 'ng2-toasty';

import { FollowService } from './follow.service';
import { User } from '../../signup/user.class';
import { customToastOptions } from '../models/toasty-options.model';
import { FollowRqstObj } from './follow.interface';


@Component({
  selector: 'app-follow',
  templateUrl: './follow.component.html',
  styleUrls: ['./follow.component.scss']
})
export class FollowComponent implements OnDestroy {
  @Input() set followedUser(userId: string) {
    this.getCurrentUser(userId);
  };
  followingSubscribe: Subscription;
  currentUser: User;
  rqstObj: FollowRqstObj;

  constructor(private followService: FollowService, private toastyService: ToastyService) {

  }

  getCurrentUser(followingId: string): void {
    try {
      this.currentUser = new User(JSON.parse(localStorage.getItem('profile')));
      this.rqstObj = {
        follower: this.currentUser._id,
        following: followingId,
      }
    } catch (err) {
      this.currentUser = null;
      console.error('something went wrong: ', err);
    }
  }

  follow(): void {
    if (!this.rqstObj.follower) {
      const toastOptions: ToastOptions = {
        ...customToastOptions,
        ...{title: 'Error', msg: `Unregistered user can't follow anything. Please sign up or sign in.`}
      };
      this.toastyService.error(toastOptions);

      return;
    }

    if (this.rqstObj.follower === this.rqstObj.following) {
      const toastOptions: ToastOptions = {
        ...customToastOptions,
        ...{title: 'Error', msg: `You can't follow on yourself`}
      };
      this.toastyService.error(toastOptions);

      return;
    }

    this.followingSubscribe = this.followService.followUser(this.rqstObj)
      .subscribe(res => {
        console.log('res', res);
        const toastOptions: ToastOptions = {
          ...customToastOptions,
          ...{title: 'Success', msg: res}
        };
        this.toastyService.success(toastOptions);
      }, err => {
        console.log('something went wrong', err);
      });
  }

  ngOnDestroy() {
    this.followingSubscribe.unsubscribe();
  }
}
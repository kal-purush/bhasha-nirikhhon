import { Injectable, OnDestroy } from '@angular/core';

import { Subscription } from 'rxjs/Subscription';
import { EnvironmentDetails } from '../../core/environment-details';
import { isDefined } from '../../core/is-defined';
import { UserInfo } from '../../core/user-info';
import { MessageService } from '../message/message.service';

@Injectable()
export class CacheService implements OnDestroy {

  private environmentDetails: EnvironmentDetails;

  user: UserInfo;
  userSubscription: Subscription;

  constructor(private messageService: MessageService) {
    this.userSubscription = this.messageService.userUpdates.subscribe(
      user => this.user = user,
      error => console.error(error),
      () => this.userSubscription.unsubscribe()
    );
  }

  getEnvironmentDetails(): Promise<EnvironmentDetails> {
    if (isDefined(this.environmentDetails)) {
      return new Promise<EnvironmentDetails>(resolve => resolve(this.environmentDetails));
    } else {
      return this.messageService.getEnvironmentDetails();
    }
  }

  ngOnDestroy(): void {
    this.userSubscription.unsubscribe();
    this.userSubscription = undefined;
  }

}
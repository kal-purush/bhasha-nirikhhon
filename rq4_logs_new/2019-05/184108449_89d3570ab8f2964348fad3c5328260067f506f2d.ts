import { Component, OnInit, OnDestroy } from '@angular/core';
import { Environment, String150 } from '@nx-angular-resume/common-classes';
import { Observable, Subject } from 'rxjs';
import { AuthFacade } from '@nx-angular-resume/auth';
import { UserFacade, User, UserId } from '@nx-angular-resume/user';
import {
  LiveChatFacade,
  Message,
  UnsendedMessage
} from '@nx-angular-resume/live-chat';
import { filter, takeUntil, map } from 'rxjs/operators';

@Component({
  selector: 'nx-angular-resume-live-chat-container',
  templateUrl: './live-chat-container.component.html',
  styleUrls: ['./live-chat-container.component.scss']
})
export class LiveChatContainerComponent implements OnInit, OnDestroy {
  user$: Observable<User>;
  destinationChatUser$: Observable<User>;
  message$: Observable<Message[]>;
  unsendedMessages$: Observable<UnsendedMessage[]>;
  annonymousMode$: Observable<boolean>;
  liveChatConnected$: Observable<boolean>;
  usersList$: Observable<User[]>;

  private unsubscribe: Subject<void> = new Subject();

  constructor(
    private authFacade: AuthFacade,
    private livechatFacade: LiveChatFacade,
    private userFacade: UserFacade,
    private env: Environment
  ) {}

  ngOnInit() {
    this.liveChatConnected$ = this.livechatFacade.connected$;
    this.unsendedMessages$ = this.livechatFacade.unsendedMessages$;
    this.livechatFacade.messages$
      .pipe(takeUntil(this.unsubscribe))
      .subscribe(message$ => {
        this.message$ = message$;
      });
    this.user$ = this.authFacade.user$.pipe(
      filter(user => (user ? true : false)),
      map(
        user =>
          ({
            uid: UserId.create(user.uid.value),
            name: user.name,
            photoURL: user.photoURL
          } as User)
      )
    );

    this.annonymousMode$ = this.authFacade.isAnnonymous$;

    this.user$.pipe(takeUntil(this.unsubscribe)).subscribe(user => {
      this.userFacade.updateUser(user);
      if (user.uid.value !== this.env.profileId)
        return this.connectLiveChatUser(
          user.uid,
          UserId.create(this.env.profileId)
        );
      this.usersList$ = this.userFacade.allUser$;
    });
  }

  ngOnDestroy() {
    this.unsubscribe.next();
    this.unsubscribe.complete();
  }

  sendMessage(message: String150): void {
    this.livechatFacade.sendMessage(message);
  }

  connectLiveChatUser(myId: UserId, destinationId: UserId): void {
    this.livechatFacade.connect(myId, destinationId);
    this.destinationChatUser$ = this.userFacade.getSelectedUser(
      destinationId.value
    );
  }

  googleSingIn(): void {
    this.authFacade.loginWithGoogle();
  }
}
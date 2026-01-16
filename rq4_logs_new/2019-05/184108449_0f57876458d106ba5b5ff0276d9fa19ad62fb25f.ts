import { Injectable } from '@angular/core';
import { AngularFirestore } from '@angular/fire/firestore';
import { Observable, from } from 'rxjs';
import { Message, UnsendedMessage } from './live-chat.public-classes';
import { IUserId, IUser } from '@nx-angular-resume/auth';
import { String150 } from '@nx-angular-resume/common-classes';
import { map, take } from 'rxjs/operators';

interface CommonTypeMessage {
  timeStamp: Date;
  destination: string;
  description: string;
}

const messageToCommon = (message: UnsendedMessage) => ({
  timeStamp: new Date(),
  destination: message.destination.value,
  description: message.description.value
});

const commonToMessage = (message: CommonTypeMessage) => ({
  ...message,
  destination: IUserId.create(message.destination),
  description: String150.create(message.description)
});

const commonToMessageAdapter = (messages: CommonTypeMessage[]) =>
  messages.map(commonToMessage);

@Injectable({
  providedIn: 'root'
})
export class LiveChatService {
  constructor(private afs: AngularFirestore) {}

  connnetToStream(
    userId: IUserId,
    destinationId: IUserId
  ): Observable<Message[]> {
    return this.afs
      .collection<CommonTypeMessage>(
        `messages/${userId.value}/${destinationId.value}`,
        ref => ref.orderBy('timeStamp').limit(50) //Available only 50 last.
      )
      .valueChanges()
      .pipe(map(commonToMessageAdapter));
  }

  createNewSession(
    userId: IUserId,
    destinationId: IUserId,
    message: UnsendedMessage
  ): Observable<boolean> {
    return from(
      this.afs
        .doc(`messages/${userId.value}`)
        .collection(destinationId.value)
        .add(messageToCommon(message))
    ).pipe(
      take(1),
      map(result => true)
    );
  }

  sendMessage(
    userId: IUserId,
    destinationId: IUserId,
    message: UnsendedMessage
  ): Observable<boolean> {
    return from(
      this.afs
        .collection<CommonTypeMessage>(
          `messages/${userId.value}/${destinationId.value}`
        )
        .add(messageToCommon(message))
    ).pipe(
      take(1),
      map(result => true)
    );
  }
}
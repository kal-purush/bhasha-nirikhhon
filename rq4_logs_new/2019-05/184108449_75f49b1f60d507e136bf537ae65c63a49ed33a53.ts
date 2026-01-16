import { String150 } from '@nx-angular-resume/common-classes';
import { UUID } from 'angular2-uuid';
import { IUserId } from '@nx-angular-resume/auth';

export class UnsendedMessageId {
  private constructor(public value: string) {}

  public static create() {
    return new UnsendedMessageId(UUID.UUID());
  }
}

export interface Message {
  timeStamp: Date;
  destination: IUserId;
  description: String150;
}

export interface UnsendedMessage {
  id: UnsendedMessageId;
  destination: IUserId;
  description: String150;
}
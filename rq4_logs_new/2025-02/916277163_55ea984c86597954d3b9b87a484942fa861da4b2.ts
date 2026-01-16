import React from 'react';
import { uuid } from '@repo/services/string/string';

export type TAlert = 'success' | 'info' | 'warning' | 'error';

type Message = React.JSX.Element | string;

export interface AlertData {
  type: TAlert;
  delay?: number;
  message: Message;
}

export default class Alert {
  public visible = true;
  public readonly id = uuid();
  public readonly type!: TAlert;
  public readonly delay: number = 5000;
  public readonly message!: Message;

  constructor({ type, delay, message }: AlertData) {
    this.type = type;
    this.message = message;

    if (delay) {
      this.delay = delay;
    }
  }
}
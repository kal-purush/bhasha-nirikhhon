import { combineLatest, Observable, Subject } from 'rxjs';
import { createContextStore, ContextStore } from '../lib';

export interface Message {
  body: string;
  date: Date;
}

export interface MessageData {
  messages: Message[];
  newCount: number;
}

export interface MessageMethods {
  addMessage(message: Message): void;
  loadMessages(): void;
  messageOne(): Observable<Message>;
  messageTwo(): Observable<Message>;
  messageThree(): Observable<Message>;
  removeAll(): void;
  removeMessage(message: Message): boolean;
}

const store: ContextStore<MessageData, MessageMethods> = createContextStore({
  data: {
    messages: [
      {
        body: 'Message',
        date: new Date(),
      },
    ],
    newCount: 1,
  },
  methods: {
    addMessage(message: Message) {
      this.data.messages = [...this.data.messages, message];
      this.data.newCount = this.data.newCount + 1;
    },
    loadMessages() {
      const obs: Observable<Message[]> = combineLatest(
        this.methods.messageOne(),
        this.methods.messageTwo(),
        this.methods.messageThree(),
      );
      obs.subscribe(messages => messages.forEach(m => this.methods.addMessage(m)));
    },
    messageOne() {
      return new Observable<Message>((observer) => {
        const message = {
          body: 'One',
          date: new Date(),
        };
        setTimeout(() => observer.next(message), 2000);
      });
    },
    messageTwo() {
      return new Observable<Message>((observer) => {
        const message = {
          body: 'Two',
          date: new Date(),
        };
        setTimeout(() => observer.next(message), 1000);
      });
    },
    messageThree() {
      return new Observable<Message>((observer) => {
        observer.next({
          body: 'Three',
          date: new Date(),
        });
      });
    },
    removeAll() {
      this.data.messages = [];
    },
    removeMessage(message: Message) {
      const messages = this.data.messages;
      const index = messages.findIndex((m: Message) => m === message);
      this.data.messages = [
        ...messages.slice(0, index),
        ...messages.slice(index + 1),
      ];
    },
  },
});

export default store;
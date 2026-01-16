declare module 'event-emitter-lite' {
  export enum EEventEmitterStatus {
    CANCELED
  }

  export interface IEventSubscribe {
    ref: string;
  }

  export interface IEventEmitterError {
    error: string;
    status: EEventEmitterStatus;
  }

  interface IEventSubscribeExtended extends IEventSubscribe {
    handler: Function;
    once?: boolean;
  }

  export interface IEventEmitter {
    done(p_onSuccess: Function, p_onError?: (err: IEventEmitterError) => any): void;
  }

  export class EventEmitter<T>{
    emit(value: T): IEventEmitter;
    subscribe(p_callback: (value: T) => any): IEventSubscribe;
    once(p_callback: (value: T) => any): IEventSubscribe;
    hasSubscribers(): boolean;
    unsubscribeAll(): void;
    cancel(): void;
    unsubscribe(subscribe: IEventSubscribe): void;
  }
}
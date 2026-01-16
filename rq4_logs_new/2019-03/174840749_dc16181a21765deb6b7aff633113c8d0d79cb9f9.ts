interface IBusOptions {
  exchangeName?: string;
  host?: string;
  log?: (msg: any, ...args: any) => void;
  password?: string;
  port?: string;
  url?: string;
  user?: string;
  vhost?: string;
}

type IMessageCallback = (event: any) => void;

declare interface IListener {
  (queueName: string, callback: IMessageCallback): void;
  (queueName: string, options: any, callback: IMessageCallback): void;
}

declare interface IPublisher {
  (queueName: string, message: object, callback?: IMessageCallback): void;
  (
    queueName: string,
    message: object,
    options: any,
    callback: IMessageCallback
  ): void;
}

declare type IMiddlewareFunction = (
  queueName: string,
  message: object,
  options: any,
  next: IMiddlewareFunction
) => void;

declare interface IMiddleware {
  handleIncoming?: IMiddlewareFunction;
  handleOutgoing?: IMiddlewareFunction;
}

interface IBus {
  send: IPublisher;
  listen: IListener;

  publish: IPublisher;
  subscribe: IListener;

  use: (middleware: IMiddleware) => void;

  close: () => void;
}

declare module 'servicebus' {
  import { Options } from 'amqplib';

  export function bus(opts?: IBusOptions, implOpts?: Options.Connect): IBus;
  export function namedBus(
    name: string,
    opts: IBusOptions,
    implOpts: Options.Connect
  ): IBus;
}
var sem = require('simple-event-machine');

export interface ISimpleEventMachine {
  on(
    eventName: string,
    callback?: (eventData?: Object, eventName?: string)=>void
  ): void
  emit(eventName: string, eventData?: Object): void
  emitSync(eventName: string, eventData?: Object): void
  Instance(injectionObject?: Object): ISimpleEventMachine
}

export class SRBEvent {
  static event: ISimpleEventMachine;

  constructor() {
    if (!SRBEvent.event) {
      SRBEvent.event          = new sem.Instance();
      //Polyfill until natively implemented.
      SRBEvent.event.emitSync = SRBEvent.event.emit;
      process.on(
        'exit', (code?: number) => {
          SRBEvent.event.emitSync('exit', code);
        }
      );
      process.on(
        'uncaughtException', (err: Error) => {
          SRBEvent.event.emitSync('uncaughtException', err);
          process.exit(1);
        }
      );
      process.on(
        'unhandledRejection', (reason: Error, p: Promise<any>) => {
          SRBEvent.warn(
            "Unhandled Rejection at: Promise ",
            p,
            " reason: ",
            reason
          );
        }
      );
    }
  }

  static debug(...data: any[]): void {
    let args = (
      arguments.length === 1 ? [arguments[0]] : Array.apply(null, arguments)
    );
    SRBEvent.event.emit(
      'debug',
      (
        args.length === 1 ? args[0] : args
      )
    );
    args.unshift('DEBUG:');
    console.info.apply(null, args);
  };

  static log(...data: any[]): void {
    let args = (
      arguments.length === 1 ? [arguments[0]] : Array.apply(null, arguments)
    );
    SRBEvent.event.emit(
      'log',
      (
        args.length === 1 ? args[0] : args
      )
    );
    console.log.apply(null, args);
  };

  static info(...data: any[]): void {
    let args = (
      arguments.length === 1 ? [arguments[0]] : Array.apply(null, arguments)
    );
    SRBEvent.event.emit(
      'info',
      (
        args.length === 1 ? args[0] : args
      )
    );
    args.unshift('INFO:');
    console.info.apply(null, args);
  };

  static warn(...data: any[]): void {
    let args = (
      arguments.length === 1 ? [arguments[0]] : Array.apply(null, arguments)
    );
    SRBEvent.event.emit(
      'warn',
      (
        args.length === 1 ? args[0] : args
      )
    );
    args.unshift('WARN:');
    console.error.apply(null, args);
  };

  static error(...data: any[]): void {
    let args = (
      arguments.length === 1 ? [arguments[0]] : Array.apply(null, arguments)
    );
    SRBEvent.event.emit(
      'error',
      (
        args.length === 1 ? args[0] : args
      )
    );
    args.unshift('ERROR:');
    console.error.apply(null, args);
  };

  /**
   * FATAL: Kills the current process after emitting error and fatal handlers
   * @param data
   */
  static fatal(...data: any[]): void {
    let args = (
      arguments.length === 1 ? [arguments[0]] : Array.apply(null, arguments)
    );
    SRBEvent.event.emitSync(
      'error',
      (
        args.length === 1 ? args[0] : args
      )
    );
    SRBEvent.event.emitSync(
      'fatal',
      (
        args.length === 1 ? args[0] : args
      )
    );
    args.unshift('FATAL:');
    console.error.apply(null, args);
    process.exit(1);
  };
}

new SRBEvent();
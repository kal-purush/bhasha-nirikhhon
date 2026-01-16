/**
 *
 */
import * as EventEmitter from 'events';
import * as $stream from 'stream';

/**
 * Base class for complex streams
 */
export class BaseComplexStream<T extends $stream.Stream> extends EventEmitter {
  public streams: T[];
  protected _opts: $stream.TransformOptions;

  constructor(streams: T[], opts: $stream.TransformOptions = {}) {
    super();
    this.streams = streams;
    this._opts = opts;
    this._bindEvents();
  }

  private _bindEvents() {
    this.on(
      'newListener',
      (event: string | symbol, listener: (...args: any[]) => void) => {
        if (event !== 'finish' && event !== 'end') {
          this.streams.forEach(stream => stream.addListener(event, listener));
        }
      }
    );
    this.on(
      'removeListener',
      (event: string | symbol, listener: (...args: any[]) => void) => {
        if (event !== 'finish' && event !== 'end') {
          this.streams.forEach(stream =>
            stream.removeListener(event, listener)
          );
        }
      }
    );
  }
}
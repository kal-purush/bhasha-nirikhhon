import {NewSignal} from 'vega-lib';

/**
 * Extend the Vega Signal specification about tracking
 */
export interface ClueSignal extends NewSignal {
  track: ITrackedSignal;
}

/**
 * Extend the Vega Signal specification about tracking
 */
export interface ITrackedSignal {

  /**
   * Title of the graph node
   * It can contain Handlebar.js syntax to replace variables.
   * @see http://handlebarsjs.com/
   *
   * Available default variables:
   * * `{{name}}`: signal name
   * * `{{value}}`: signal value
   *
   * Further variables can be added by using the `async` option.
   */
  title: string;

  /**
   * If set wait for completing dataflow evaluation
   */
  async?: TrackedSignalAsync[];

}

interface IBaseAsync {
  /**
   * If set use this alias name for replacement in the title
   */
  as?: string;
}

export interface IAsyncData extends IBaseAsync {
  /**
   * A valid dataset name
   */
  data: string;
}

export interface IAsyncSignal extends IBaseAsync {
  /**
   * A valid signal name
   */
  signal: string;
}

type TrackedSignalAsync = IAsyncData | IAsyncSignal;


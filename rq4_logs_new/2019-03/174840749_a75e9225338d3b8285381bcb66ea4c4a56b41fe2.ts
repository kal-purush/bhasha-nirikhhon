/* tslint:disable max-classes-per-file function-name */

interface ICommandResultState {
  SUCCESS: 'success';
  ERROR: 'error';
}

interface IErrorSerialization {
  message: string;
  name: string;
  stack: string;
  code: string;
  fileName: string;
  lineNumber: number;
  columnNumber: number;
}

declare module '@elastic.io/amqp-rpc' {
  import { Channel, Connection, Message } from 'amqplib';
  import assert from 'assert';
  import EventEmitter from 'events';

  type CommandCallback = (c: Command) => void;

  export interface IAMQPEventsReceiverOpts {
    queueName?: string;
  }

  export interface IAMQPServerOpts {
    requestsQueue?: string;
  }

  export interface IAMQPClientOpts {
    requestsQueue: string;
    repliesQueue?: string;
    timeout?: number;
    defaultMessageOptions: object;
  }

  /**
   * Base class for AMQPRPCServer/AMQPRPCClient.
   *
   * @class
   */
  export class AMQPEndpoint {
    /**
     *
     * @param {*} connection Connection reference created from `amqplib` library
     *
     * @param {Object} [params]
     */

    private _channel?: Channel;

    constructor(connection: Connection, params?: {});

    /**
     * Initialization before starting working
     * NOTE! Race condition is not handled here,
     *    so it's better to not invoke the method several times (e.g. from multiple "threads")
     *
     * @return {Promise<void>}
     */
    public start(): Promise<void>;

    /**
     * Opposite to this.start() – clearing
     * NOTE! Race condition is not handled here,
     *    so it's better to not invoke the method several times (e.g. from multiple "threads")
     *
     * @return {Promise<void>}
     */
    public disconnect(): Promise<void>;
  }

  /**
   * @class AMQPEventsReceiver
   * Provides stream-like "endpoint" that transforms sequence of messages in amqp queue
   * into sequence of 'data' events.
   * Should be used in pair with AMQPEventsSender class
   * In such case provides end/close events that imitate nodejs's ReadableStream,
   * and cleaning of used amqp resources (queue)
   * @emits AMQPEventsReceiver#data
   * @emits AMQPEventsReceiver#close
   * @emits AMQPEventsReceiver#end
   */

  export class AMQPEventsReceiver extends EventEmitter {
    /**
     * Allows to get generated value when params.repliesQueue was set to '' (empty string) or omitted
     * @returns {String} an actual name of the queue used by the instance for receiving replies
     */
    public queueName: string;

    private _params?: IAMQPEventsReceiverOpts;
    private _channel?: Channel;

    /**
     * @constructor
     * @param {amqplib.Connection} amqpConnection
     * @param {Object} params
     * @param {String} [params.queueName=''] queue for receiving events, should correspond with AMQPEventsSender
     *    default is '' which means auto-generated queue name, should correspond with AMQPEventsSender
     */
    constructor(connect: Connection, params?: IAMQPEventsReceiverOpts);

    /**
     * Begin to listen for messages from amqp
     * @returns {Promise<String>} name of endpoint to send messages
     * @override
     */
    public start(): Promise<string>;

    /**
     * Stop listening for messages
     * @override
     */
    public disconnect(): Promise<void>;

    private _handleMessage(msg: Message): void;
  }

  /**
   * This class is responsible for wrapping command structure for sending across queues.
   * It uses when you need to send a command request to an RPC queue in Rabbit.
   *
   * @class
   */
  export class Command {
    /**
     * Static helper for creating new instances of a Command.
     *
     * @static
     * @param args
     * @returns {Command}
     */
    public static create(...args: [string, ...object[]]): Command;

    /**
     * Static helper for creating new Command instances.
     *
     * @static
     * @param {Buffer} buffer
     * @returns {Command}
     */
    public static fromBuffer(buffer: Buffer): Command;

    /**
     * Creates a new command instance.
     *
     * @param {String} command RPC command name
     * @param {Array<*>} args Array of arguments to provide an RPC
     * @example
     * const command = new Command('commandName', [
     *  {foo: 'bar'},
     *  [1, 2, 3]
     * ]);
     */
    constructor(command: string, args: object[]);

    /**
     * Pack a command into the buffer for sending across queues.
     *
     * @returns {Buffer}
     */
    public pack(): Buffer;
  }

  /**
   * This class is responsible for (de)serializing results of a specific commands.
   * Instances of this class are sent by {@link AMQPRPCServer} in response to command requests.
   *
   * @class
   */
  export class CommandResult {
    /**
     * Returns a dictionary of possible STATES in the result.
     *
     * @static
     * @returns {{SUCCESS: String, ERROR: String}}
     */
    public static STATES: ICommandResultState;

    /**
     * Static helper for creating new instances of {@link CommandResult}.
     *
     * @static
     * @param args
     * @returns {CommandResult}
     */
    public static create(...args: [string, ...any[]]): CommandResult;

    /**
     * Static helper for creating a new instance of {@link CommandResult} from Buffer.
     *
     * @static
     * @param {Buffer} buffer
     * @returns {CommandResult}
     * @example
     * const commandResult = CommandResult.fromBuffer({state: CommandResult.STATES.SUCCESS, data: []});
     * const buffer = commandResult.pack();
     *
     * assert.instanceOf(buffer, Buffer);
     * assert.deepEqual(CommandResult.fromBuffer(buffer), commandResult);
     */
    public static fromBuffer(buffer: Buffer): CommandResult;

    /**
     * Simple traverse function for `JSON.stringify`.
     *
     * @static
     * @private
     * @param {String} key
     * @param {*} value
     * @returns {*}
     */
    private static _replacer<T>(
      key: string,
      value: T | Error
    ): IErrorSerialization | T;

    /**
     * Creates a new instance of a command result.
     *
     * @param {String} state State from {@link CommandResult.STATES}
     * @param {*} data Any type that can be understandable by `JSON.stringify`
     * @example
     * const commandResult = new CommandResult({
     *  state: CommandResult.STATES.ERROR,
     *  data: new Error('Some error description'),
     * });
     *
     * const commandResult = new CommandResult({
     *  state: CommandResult.STATES.SUCCESS,
     *  data: ['some', 'data', 'here'],
     * });
     */
    constructor(state: string, data: any);

    /**
     * Packs a command result into the buffer for sending across queues.
     *
     * @returns {Buffer}
     */
    public pack(): Buffer;
  }

  /**
   * Implementation for an AMQP RPC server.
   *
   * @class
   */
  class AMQPRPCServer extends AMQPEndpoint {
    /**
     * Allows to get generated value when params.requestsQueue was set to '' (empty string) or omitted
     * @returns {String} an actual name of the queue used by the instance for receiving replies
     */

    public requestsQueue: string;

    private _commands: { [s: string]: CommandCallback };

    /**
     * Creates a new instance of RPC server.
     *
     * @param {*} connection Connection reference created from `amqplib` library
     *
     * @param {Object} params
     * @param {String} params.requestsQueue queue when AMQPRPC client sends commands, should correspond with AMQPRPCClient
     *    default is '' which means auto-generated queue name
     */
    constructor(connection: Connection, params: IAMQPServerOpts);

    /**
     * Initialize RPC server.
     *
     * @returns {Promise}
     * @override
     */
    public start(): Promise<void>;
    /**
     * Opposite to this.start()
     *
     * @returns {Promise}
     */
    public disconnect(): Promise<void>;

    /**
     * Registers a new command in this RPC server instance.
     *
     * @param {String} command Command name
     * @param {Function} cb Callback that must be called when server got RPC command
     * @returns {AMQPRPCServer}
     */
    public addCommand(command: string, cb: CommandCallback): AMQPRPCServer;

    /**
     *
     * @private
     */
    private _handleMsg(msg: Message): Promise<void>;

    /**
     * Dispatches a command with specified message.
     *
     * @private
     * @param {Object} msg
     */
    private _dispatchCommand(msg: Message): Promise<void>;
  }

  /**
   * This class is responsible for sending commands to the RPC server.
   *
   * @class
   */
  class AMQPRPCClient extends AMQPEndpoint {
    /**
     * Allows to get generated value when params.repliesQueue was set to '' (empty string) or omitted
     * @returns {String} an actual name of the queue used by the instance for receiving replies
     */
    public repliesQueue: string;

    /**
     * Returns a timeout for a command result retrieval.
     *
     * @static
     * @returns {Number}
     */
    public TIMEOUT(): number;

    /**
     * Creates a new instance of an RPC client.
     *
     * @param {*} connection Instance of `amqplib` library
     *
     * @param {Object} params
     * @param {String} params.requestsQueue queue for sending commands, should correspond with AMQPRPCServer
     * @param {String} [params.repliesQueue=''] queue for feedback from AMQPRPCServer,
     *    default is '' which means auto-generated queue name
     * @param {Number} [params.timeout=60000] Timeout for cases when server is not responding
     * @param {Object} [params.defaultMessageOptions] additional options for publishing the request to the queue
     */
    constructor(connection: Connection, params: IAMQPClientOpts);

    /**
     * Send a command into RPC queue.
     *
     * @param {String} command Command name
     * @param [Array<*>] args Array of any arguments provided to the RPC server callback
     * @param [Object] messageOptions options for publishing the request to the queue
     * @returns {Promise<*>}
     * @example
     * client.sendCommand('some-command-name', [{foo: 'bar'}, [1, 2, 3]]);
     */
    public sendCommand(
      command: string,
      args: any[],
      messageOptions: { [s: string]: any }
    ): Promise<void>;

    /**
     * Initialize RPC client.
     *
     * @returns {Promise}
     * @override
     */
    public start(): Promise<void>;

    /**
     * Opposite to this.start()
     *
     * @returns {Promise}
     */
    public disconnect(): Promise<void>;

    /**
     * Replies handler
     * @param {Object} msg, returned by channel.consume
     * @private
     * @returns {Promise}
     */
    private _dispatchReply(msg: Message): Promise<void>;

    private _cancel(correlationId: string, reason: string): Promise<void>;
  }
}
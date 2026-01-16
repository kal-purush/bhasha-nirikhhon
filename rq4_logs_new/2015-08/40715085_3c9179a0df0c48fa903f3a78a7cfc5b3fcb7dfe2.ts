/*-----------------------------------------------------------------------------
| Copyright (c) 2014-2015, PhosphorJS Contributors
|
| Distributed under the terms of the BSD 3-Clause License.
|
| The full license is in the file LICENSE, distributed with this software.
|----------------------------------------------------------------------------*/

/**
 * An object used for type-safe inter-object communication.
 *
 * #### Notes
 * User code will not create a signal object directly, instead one will
 * be returned when accessing the property defined by the `@signal`
 * decorator.
 *
 * When defining a signal from plain JS (where decorators may not be
 * supported), the `signal` function can be invoked directly with the
 * class prototype and signal name.
 *
 * #### Example
 * ```typescript
 * class SomeClass {
 *
 *   @signal
 *   valueChanged: ISignal<number>;
 *
 * }
 *
 * // ES5 alternative signal definition
 * signal(SomeClass.prototype, 'valueChanged');
 * ```
 */
export
interface ISignal<T> {
  /**
   * Connect a callback to the signal.
   *
   * @param callback - The function to invoke when the signal is
   *   emitted. The args object emitted with the signal is passed
   *   as the first and only argument to the function.
   *
   * @param thisArg - The object to use as the `this` context in the
   *   callback. If provided, this must be a non-primitive object.
   *
   * @returns `true` if the connection succeeds, `false` otherwise.
   *
   * #### Notes
   * Connected callbacks are invoked synchronously, in the order in
   * which they are connected.
   *
   * Signal connections are unique. If a connection already exists for
   * the given `callback` and `thisArg`, this function returns `false`.
   *
   * A newly connected callback will not be invoked until the next time
   * the signal is emitted, even if it is connected while the signal is
   * being emitted.
   *
   * #### Example
   * ```typescript
   * // connect a method
   * someObject.valueChanged.connect(myObject.onValueChanged, myObject);
   *
   * // connect a plain function
   * someObject.valueChanged.connect(myCallback);
   * ```
   */
  connect(callback: (args: T) => void, thisArg?: any): boolean;

  /**
   * Disconnect a callback from the signal.
   *
   * @param callback - The callback connected to the signal.
   *
   * @param thisArg - The `this` context for the callback.
   *
   * @returns `true` if the connection is broken, `false` otherwise.
   *
   * #### Notes
   * A disconnected callback will no longer be invoked, even if it
   * is disconnected while the signal is being emitted.
   *
   * If no connection exists for the given `callback` and `thisArg`,
   * this function returns `false`.
   *
   * #### Example
   * ```typescript
   * // disconnect a method
   * someObject.valueChanged.disconnect(myObject.onValueChanged, myObject);
   *
   * // disconnect a plain function
   * someObject.valueChanged.disconnect(myCallback);
   * ```
   */
  disconnect(callback: (args: T) => void, thisArg?: any): boolean;

  /**
   * Emit the signal and invoke the connected callbacks.
   *
   * @param args - The args object to pass to the callbacks.
   *
   * #### Notes
   * If a connected callback throws an exception, dispatching of the
   * signal will terminate immediately and the exception will be
   * propagated to the call site of this function.
   *
   * #### Example
   * ```typescript
   * someObject.valueChanged.emit(42);
   * ```
   */
  emit(args: T): void;
}


/**
 * A decorator which defines a signal for an object.
 *
 * @param proto - The object prototype on which to define the signal.
 *
 * @param name - The name of the signal to define.
 *
 * #### Notes
 * When defining a signal from plain JS (where decorators may not be
 * supported), this function can be invoked directly with the class
 * prototype and signal name.
 *
 * #### Example
 * ```typescript
 * class SomeClass {
 *
 *   @signal
 *   valueChanged: ISignal<number>;
 *
 * }
 *
 * // ES5 alternative signal definition
 * signal(SomeClass.prototype, 'valueChanged');
 * ```
 */
export
function signal(proto: any, name: string): void {
  var token = {};
  Object.defineProperty(proto, name, {
    get: function() { return new Signal(this, token); },
  });
}


/**
 * Get the object which is emitting the curent signal.
 *
 * @returns The object emitting the current signal, or `null` if a
 *   signal is not currently being emitted.
 *
 * #### Example
 * ```typescript
 * someObject.valueChanged.connect(myCallback);
 *
 * someObject.valueChanged.emit(42);
 *
 * function myCallback(value: number): void {
 *   console.log(emitter() === someObject); // true
 * }
 * ```
 */
export
function emitter(): any {
  return currentEmitter;
}


/**
 * Remove all connections where the given object is the emitter.
 *
 * @param obj - The emitter object of interest.
 *
 * #### Example
 * ```typescript
 * disconnectEmitter(someObject);
 * ```
 */
export
function disconnectEmitter(obj: any): void {
  var list = emitterMap.get(obj);
  if (!list) {
    return;
  }
  var conn = list.first;
  while (conn !== null) {
    removeFromEmittersList(conn);
    conn.callback = null;
    conn.thisArg = null;
    conn = conn.nextReceiver;
  }
  emitterMap.delete(obj);
}


/**
 * Remove all connections where the given object is the receiver.
 *
 * @param obj - The receiver object of interest.
 *
 * #### Notes
 * If a `thisArg` is provided when connecting a signal, that object
 * is considered the receiver. Otherwise, the `callback` is used as
 * the receiver.
 *
 * #### Example
 * ```typescript
 * // disconnect a regular object receiver
 * disconnectReceiver(myObject);
 *
 * // disconnect a plain callback receiver
 * disconnectReceiver(myCallback);
 * ```
 */
export
function disconnectReceiver(obj: any): void {
  var conn = receiverMap.get(obj);
  if (!conn) {
    return;
  }
  while (conn !== null) {
    var next = conn.nextEmitter;
    conn.callback = null;
    conn.thisArg = null;
    conn.prevEmitter = null;
    conn.nextEmitter = null;
    conn = next;
  }
  receiverMap.delete(obj);
}


/**
 * Clear all signal data associated with the given object.
 *
 * @param obj - The object for which the signal data should be cleared.
 *
 * #### Notes
 * This removes all signal connections where the object is used as
 * either the emitter or the receiver.
 *
 * #### Example
 * ```typescript
 * clearSignalData(someObject);
 * ```
 */
export
function clearSignalData(obj: any): void {
  disconnectEmitter(obj);
  disconnectReceiver(obj);
}


/**
 * A concrete implementation of ISignal.
 */
class Signal implements ISignal<any> {
  /**
   * Construct a new signal.
   */
  constructor(owner: any, token: any) {
    this._owner = owner;
    this._token = token;
  }

  /**
   * Connect a callback to the signal.
   */
  connect(callback: (args: any) => void, thisArg?: any): boolean {
    return connect(this._owner, this._token, callback, thisArg);
  }

  /**
   * Disconnect a callback from the signal.
   */
  disconnect(callback: (args: any) => void, thisArg?: any): boolean {
    return disconnect(this._owner, this._token, callback, thisArg);
  }

  /**
   * Emit the signal and invoke the connected callbacks.
   */
  emit(args: any): void {
    emit(this._owner, this._token, args);
  }

  private _owner: any;
  private _token: any;
}


/**
 * A struct which holds connection data.
 */
class Connection {
  /**
   * The token which identifies the signal.
   */
  token: any = null;

  /**
   * The callback connected to the signal.
   */
  callback: Function = null;

  /**
   * The `this` context for the callback.
   */
  thisArg: any = null;

  /**
   * The next connection in the singly linked receivers list.
   */
  nextReceiver: Connection = null;

  /**
   * The next connection in the doubly linked emitters list.
   */
  nextEmitter: Connection = null;

  /**
   * The previous connection in the doubly linked emitters list.
   */
  prevEmitter: Connection = null;
}


/**
 * The list of receiver connections for a specific emitter.
 */
class ConnectionList {
  /**
   * The ref count for the list.
   */
  refs = 0;

  /**
   * The first connection in the list.
   */
  first: Connection = null;

  /**
   * The last connection in the list.
   */
  last: Connection = null;
}


/**
 * A mapping of emitter object to its receiver connection list.
 */
var emitterMap = new WeakMap<any, ConnectionList>();


/**
 * A mapping of receiver object to its emitter connection list.
 */
var receiverMap = new WeakMap<any, Connection>();


/**
 * The object emitting the current signal.
 */
var currentEmitter: any = null;


/**
 * Connect a signal to a callback.
 */
function connect(owner: any, token: any, callback: Function, thisArg: any): boolean {
  // Coerce a `null` thisArg to `undefined`.
  thisArg = thisArg || void 0;

  // Search for an equivalent connection and bail if one is found.
  var list = emitterMap.get(owner);
  if (list && findConnection(list, token, callback, thisArg)) {
    return false;
  }

  // Create a new connection.
  var conn = new Connection();
  conn.token = token;
  conn.callback = callback;
  conn.thisArg = thisArg;

  // Add the connection to the receivers list.
  if (!list) {
    list = new ConnectionList();
    list.first = conn;
    list.last = conn;
    emitterMap.set(owner, list);
  } else {
    list.last.nextReceiver = conn;
    list.last = conn;
  }

  // Add the connection to the emitters list.
  var receiver = thisArg || callback;
  var head = receiverMap.get(receiver);
  if (head) {
    head.prevEmitter = conn;
    conn.nextEmitter = head;
  }
  receiverMap.set(receiver, conn);

  return true;
}


/**
 * Disconnect a signal from a callback.
 */
function disconnect(owner: any, token: any, callback: Function, thisArg: any): boolean {
  // Coerce a `null` thisArg to `undefined`.
  thisArg = thisArg || void 0;

  // Bail early if there is no equivalent connection.
  var list = emitterMap.get(owner);
  if (!list) {
    return false;
  }
  var conn = findConnection(list, token, callback, thisArg);
  if (!conn) {
    return false;
  }

  // Remove the connection from the emitters list. It will be removed
  // from the receivers list the next time the signal is emitted.
  removeFromEmittersList(conn);

  // Clear the connection data so it becomes a dead connection.
  conn.callback = null;
  conn.thisArg = null;

  return true;
}


/**
 * Emit a signal and invoke the connected callbacks.
 */
function emit(owner: any, token: any, args: any): void {
  var list = emitterMap.get(owner);
  if (!list) {
    return;
  }
  var prev = currentEmitter;
  currentEmitter = owner;
  list.refs++;
  try {
    var dirty = invokeList(list, token, args);
  } finally {
    currentEmitter = prev;
    list.refs--;
  }
  if (dirty && list.refs === 0) {
    cleanList(list);
  }
}


/**
 * Find a matching connection in the given connection list, or null.
 */
function findConnection(list: ConnectionList, token: any, callback: Function, thisArg: any): Connection {
  var conn = list.first;
  while (conn !== null) {
    if (conn.token === token &&
        conn.callback === callback &&
        conn.thisArg === thisArg) {
      return conn;
    }
    conn = conn.nextReceiver;
  }
  return null;
}


/**
 * Invoke the matching callbacks in the given connection list.
 *
 * Connections added during dispatch will not be invoked. This returns
 * `true` if there are dead connections in the list, `false` otherwise.
 */
function invokeList(list: ConnectionList, token: any, args: any): boolean {
  var dirty = false;
  var last = list.last;
  var conn = list.first;
  while (conn !== null) {
    if (conn.callback && conn.token === token) {
      conn.callback.call(conn.thisArg, args);
    } else if (!conn.callback) {
      dirty = true;
    }
    if (conn === last) {
      break;
    }
    conn = conn.nextReceiver;
  }
  return dirty;
}


/**
 * Remove the dead connections from the given connection list.
 */
function cleanList(list: ConnectionList): void {
  var prev: Connection;
  var conn = list.first;
  while (conn !== null) {
    var next = conn.nextReceiver;
    if (!conn.callback) {
      conn.nextReceiver = null;
    } else if (!prev) {
      list.first = conn;
      prev = conn;
    } else {
      prev.nextReceiver = conn;
      prev = conn;
    }
    conn = next;
  }
  if (!prev) {
    list.first = null;
    list.last = null;
  } else {
    prev.nextReceiver = null;
    list.last = prev;
  }
}


/**
 * Remove a connection from the doubly linked list of senders.
 */
function removeFromEmittersList(conn: Connection): void {
  var receiver = conn.thisArg || conn.callback;
  var prev = conn.prevEmitter;
  var next = conn.nextEmitter;
  if (prev === null && next === null) {
    receiverMap.delete(receiver);
  } else if (prev === null) {
    receiverMap.set(receiver, next);
    next.prevEmitter = null;
  } else if (next === null) {
    prev.nextEmitter = null;
  } else {
    prev.nextEmitter = next;
    next.prevEmitter = prev;
  }
  conn.prevEmitter = null;
  conn.nextEmitter = null;
}
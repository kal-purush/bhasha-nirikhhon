/**
 * Flags to signifiy how a function implements asynchronous behavior
 * @enum {number}
 * @prop {number} PROMISE Function returns a promise
 * @prop {number} TWO_CBS Function takes two callback functions, the first for success and the
 *     second for failure.
 * @prop {number} DOT_FAIL Function takes one callback function, which is used for success.  This
 *     callback function has a property `fail`, which is the callback function for failure
 * @prop {number} NODE Function takes one callback function.  If the first argument to that
 *     callback funtion is `null` or `undefined`, that signifies success and the second argument
 *     should be used for the return value.  If the first argument is something else, that
 *     signifies failure and the first argument is the error to return.
 * @prop {number} INFER Function may or may not take a callback function.  If it takes one or
 *     more arguments, assume it uses `TWO_CBS` style.  Otherwise, it uses `NODE` style.
 */
export enum AsyncStyle {
  PROMISE,
  TWO_CBS,
  DOT_FAIL,
  NODE,
  INFER = undefined
}
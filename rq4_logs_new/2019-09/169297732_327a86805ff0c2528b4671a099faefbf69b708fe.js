/**
 * Maintains an internal counter of time for when to execute the supplied
 * callback with the arguments. Every time the debounced function is invoked,
 * the internal timer resets so as to only ever execute once if called multiple
 * times.
 *
 * Takes duration as the first argument (to facilitate partially applied
 * debounced actions), then expects a callback function as the second argument
 * which then returns a function that expects the arguments that will be passed
 * to the callback function.
 *
 * These are the behaviours and properties you can expect of this function:
 * * will only invoke the callback once
 * * callback will be invoked when the delay timer expires
 * * calling the debounced function multiple times resets the timer
 *
 * Even when called multiple times, this function will only execute the
 * callback fn once after waiting for duration of time. Each time this function
 * is called, the duration counter is reset.
 *
 * @param {Number} msToWaitForRequests
 *
 *   The amount of time that must elapse before the invoked debounced function
 *   will fire off. Any future invocations (of the debounced callback function)
 *   that occur before the elapsed time runs out will reset the countdown
 *   timer.
 *
 * @return {function(callback: CallableFunction): function(...args):
 *   Promise<callback>}
 *
 *   Returns a function that expects a `callback` function that returns a
 *   function that expects a list of arguments to supply to the callback
 *   function. Finally, returns the promisified callback function after waiting
 *   `msToWaitForRequests` milliseconds.
 *
 * @example
 *
 * import { debounce } from "holdon";
 *
 * const greet = name => `hello ${name}`; // this is the callback function
 *
 * const ms = 2000;
 * const debounceTwoSeconds = debounce(ms);
 * const greetInTwoSeconds = debounceTwoSeconds(greet); // or all at once `debounce(2000)(greet);`
 *
 * greetInTwoSeconds("John"); // countdown timer = +2000 ms
 * await delay(1000);         // countdown timer = +1000 ms
 * greetInTwoSeconds("John"); // countdown timer = +2000 ms
 * await delay(1500);         // countdown timer = +500 ms
 * await delay(400);          // countdown timer = +100 ms
 * await delay(99);           // countdown timer = +1 ms
 * greetInTwoSeconds("John"); // countdown timer = +2000 ms (callback has never fired yet)
 *
 * // ...two seconds later
 * greetInTwoSeconds("John"); //=> "hello John" (two seconds after the last fn call is made to `greetInTwoSeconds(...args)`
 *
 */
function debounce(msToWaitForRequests) {
  return function(callback) {
    // the `countdownTimer` remains outside the closure of the subsequent
    // returned function so that it only ever exists once for all invocations
    // of the debounced function
    let countdownTimer;

    return function(...args) {
      return new Promise(function(resolve) {
        clearTimeout(countdownTimer);
        countdownTimer = setTimeout(function() {
          countdownTimer = null;
          resolve(callback(...args));
        }, msToWaitForRequests);
      });
    };
  };
}

export { debounce };
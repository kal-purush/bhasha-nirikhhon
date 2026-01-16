/* 
Debouncing : Debounce function limits the execution of a function call and 
waits for a certain amount of time before running it again.

Implementing Debounce:
The general idea for debouncing is-
1) Start with 0 timeout.
2) If the debounced function is called again, reset the timer to the specified delay.
3) In case of timeout, call the debounced function.
*/

let debounce = function (func, delay) {
    let timer;
    return function (...args) {
        let context = this;
        clearTimeout(timer);
        timer = setTimeout(() => {
            // console.log('In the log of timeout')
            func.apply(context, args)
        }, delay);
    }
}

/*
Throttling is a technique in which the attached function will be executed only once 
in a given time interval. No matter how many times the user fires the event, 
only the first fired event is executed immediately. Throttling gives us control over the rate 
at which a function is executed. With this, we can optimize the performance of the app 
by limiting the number of calls per interval.
*/

let throtling = function (func, delay) {
    let isTriggered;
    return function (...args) {
        if (!isTriggered) {
            isTriggered = true;
            let context = this;
            // clearTimeout(timer);
            timer = setTimeout(() => {
                func.apply(context, args);
                isTriggered = false;
            }, delay);
        }
    }
}
"use strict;"

// We need to know how many milliseconds are in 
// different time units to perform our time calculations.
// Since these don't change, we'll save them as constants.
const MS_IN_SEC = 1000;
const MS_IN_MIN = 60 * MS_IN_SEC;
const MS_IN_HR = 60 * MS_IN_MIN;
const MS_IN_DAY = 24 * MS_IN_HR;

/** @function createCountdown 
 * This function creates a dynamic countdown clock and 
 * attaches it to the DOM.
 * @param {String} elementID - the id attribute of the 
 * DOM node to append the clock to
 * @param {Date} startTime - the date to count down to 
 */
function createCountdown(elementID, startTime) {

    // We create a new element (in this case, a div)
    // to be our clock on the page.
    var clock = document.createElement('div');

    /** @function setTime
     * This helper function sets the contents of our 
     * div declared above.  Because we declared it within
     * the scope of createCountdown, a new copy of setTime 
     * is created every time createCountdown is called,  
     * tied to different clock variables.
     * We wouldn't want to use this pattern if createCountdown 
     * is called a lot... but since we're only doing it twice, 
     * this works well to separate the countdown process for 
     * the two clocks.
     */
    function setTime() {
        var time = startTime - Date.now();
        var days = Math.floor(time / MS_IN_DAY); 
        time -= days * MS_IN_DAY;
        var hours = Math.floor(time / MS_IN_HR);
        time -= hours * MS_IN_HR;
        var minutes = Math.floor(time / MS_IN_MIN);
        time -= minutes * MS_IN_MIN; 
        var seconds = Math.floor(time / MS_IN_SEC);

        // We can access the clock variable from the outer
        // scope 
        clock.innerHTML = "In " + days + " days " 
        + hours + " hours "
        + minutes + " minutes " 
        + seconds + " seconds";
    }

    // We call setTime immediately to publish the countdown 
    setTime();

    // And then we call it every 1000ms to advance the clock
    setInterval(setTime, 1000);

    // Finally, we append our clock to the DOM node passed into 
    // us to render the clock on the page.
    document.getElementById(elementID).appendChild(clock);
}

// Now that we've defined createCountdown, we need to 
// invoke it for each countdown clock we want to create.
createCountdown('all-university', Date.parse('2018-4-7'));
createCountdown('engineering', Date.parse('2018-4-6'));




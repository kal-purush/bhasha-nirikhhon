//
// Special Note.  This code is NOT encapsulated.
// It is loaded by bootBot and runs under the auspices
// of the browser's bookmark that issued an initial
// $.getScript that starts the whole chain of JavaScript code.

//
// Currently, this Bot is 99% event driven.
//
// The events are defined and supported by the dispatcher.
//
// This little bit of procedural logic is here to build
// the necessary enviornment for the dispatcher and for
// user functions.
//

// keep the user informed, for debugging purposes only
alert("starting bot");

// Define and let's build the command table
var t = new commandTable();

// establish the concept of having a dispatcher
var d: Dispatcher;

// Now, allocate the dispatcher, and let the events
// run their course
d = new Dispatcher();

// Special Note.  This code is NOT encapsulated.

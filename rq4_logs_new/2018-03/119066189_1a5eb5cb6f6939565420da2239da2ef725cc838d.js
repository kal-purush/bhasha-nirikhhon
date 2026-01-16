var mixEvents = function(obj) {
  //Your code here
  var events = {};

  obj.trigger = (event, ...args) => {
    if (events[event]) {
      for (const eventlet of events[event]) {
        eventlet.bind(obj)(...args);
      }
    }
  };

  // Register a callback to be fired on this event.
  obj.on = (event, callback) => {
    events[event] = !events[event] ? [callback] : [...events[event], callback];
  };

  return obj;
};
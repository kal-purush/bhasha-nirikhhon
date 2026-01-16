const debounce = function(fn, delay) {
  var prevId = null;
  return function(...args) {
    if(prevId) {
      clearTimeout(prevId);
    }
    prevId = setTimeout(() => {
      fn(...args);
      prevId = null;
    }, delay)
  }
}

const throttle = function(fn, delay, atLeast) {
  var prevId = null;
  var timer = null;
  return function(...args) {
    if(!timer) timer = Date.now();
    if((Date.now() - timer) > atLeast) {
      fn(...args)
      timer = Date.now()
    } else {
      if(prevId) {
        clearTimeout(prevId)
      }
      prevId = setTimeout(() => {
        fn(...args)
        prevId = null
      }, delay)
    }
  }
}
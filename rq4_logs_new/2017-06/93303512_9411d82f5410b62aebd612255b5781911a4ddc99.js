function curry(fn) {
  return (...args) => {
    console.log(...args);
    let last = args[args.length - 1];
    if (typeof last === 'function') {
      return fn(...args);
    }
    return Component => fn(...args, Component);
  };
}




var s = curry(function add(sum1,sum2){
  return sum1+sum2;

});

s(2,3);

var q = function(defaultClass, Component){
  let propTypes = Component.propTypes || (Component.propTypes = {});
  let defaultProps = Component.defaultProps || (Component.defaultProps = {});

  propTypes.bsClass = PropTypes.string;
  defaultProps.bsClass = defaultClass;

  return Component;
}

var bsClass = function(...args) => {
  let last = args[args.length - 1];
  if (typeof last === 'function') {
    return q(...args);
  }
  return Component => q(...args, Component);
};
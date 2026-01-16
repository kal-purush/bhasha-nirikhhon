var topEnv = Object.create(null);

topEnv['true'] = true;
topEnv['false'] = false;

['+', '-', '*', '/', '==', '<', '>'].forEach(function(op) {
  topEnv[op] = new Function('a, b', 'return a ' + op + ' b;');
});

topEnv['plus'] = function(a, b) {
  return a + b;
};

topEnv['minus'] = function(a, b) {
  return a - b;
};

topEnv['multiply'] = function(a, b) {
  return a * b;
};

topEnv['divide'] = function(a, b) {
  return a / b;
};

topEnv['compare'] = function(a, b) {
  return a == b;
};

topEnv['less'] = function(a, b) {
  return a < b;
};

topEnv['more'] = function(a, b) {
  return a > b;
};

topEnv['print'] = function(value) {
  console.log(value);
  return value;
};

topEnv['module'] = function(number) {
  return Math.abs(number);
};

topEnv['random'] = function(min, max) {
  return Math.random() * (max - min) + min;
};

topEnv['array'] = function() {
  return Array.prototype.slice.call(arguments, 0);
};

topEnv['length'] = function(array) {
  return array.length;
};

topEnv['element'] = function(array, i) {
  return array[i];
};

module.exports = topEnv;
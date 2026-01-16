/*     DScript.js 1.0.0
*     (c) 2017 Deepak Ranjan Jena, Frontend Atchitect
*     DScript may be freely distributed under the MIT license.
*/

(() => {
  // Establish the root object, `window` (`self`) in the browser, `global`
  // on the server, or `this` in some virtual machines. We use `self`
  // instead of `window` for `WebWorker` support.  
  var root = typeof self == 'object' && self.self === self && self ||
            typeof global == 'object' && global.global === global && global ||
            this ||
            {};
  
  // Create a safe reference to the DScript object for use below.
  var DS = function(obj) {
    if (obj instanceof DS) return obj;
    if (!(this instanceof DS)) return new DS(obj);
  };
  
  // Export the DScript object for **Node.js**, with
  // backwards-compatibility for their old module API. If we're in
  // the browser, add `DS` as a global object.
  // (`nodeType` is checked to ensure that `module`
  // and `exports` are not HTML elements.)
  
  if (typeof exports != 'undefined' && !exports.nodeType) {
    if (typeof module != 'undefined' && !module.nodeType && module.exports) {
      exports = module.exports = DS;
    }
    exports.DS = DS;
  } else {
    root.DS = DS;
  }
  
  // Current version.
  DS.VERSION = '1.0.0';
})();
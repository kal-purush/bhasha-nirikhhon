/**
 * Namespace
 */
neotree = {};
  
/**
 * Hilariously shitty loader.
 */
neotree.deps = [
    "neotree-app.js",
    "tree-configuration-screen.js",
    "tree-led-finder.js",
    "utils.js",
    "effects.js"
  ];
neotree.readyCallbacks_ = [];
neotree.ready = function(fn) {
  neotree.readyCallbacks_.push(fn);
};

jQuery(function() {
  var loadedScripts = 0;
  neotree.deps.forEach(function(script) {
    var s = document.createElement('script');
    s.src = '/resources/neotree/' + script;
    s.onload = function() {
      loadedScripts += 1;
      if (loadedScripts == neotree.deps.length) {
	neotree.readyCallbacks_.forEach(function(fn) {
	  fn();
	});
      }
    };
    document.body.appendChild(s);
  });
});
/******/ (function(modules) { // webpackBootstrap
/******/ 	// The module cache
/******/ 	var installedModules = {};
/******/
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/
/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId]) {
/******/ 			return installedModules[moduleId].exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			i: moduleId,
/******/ 			l: false,
/******/ 			exports: {}
/******/ 		};
/******/
/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/
/******/ 		// Flag the module as loaded
/******/ 		module.l = true;
/******/
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/
/******/
/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;
/******/
/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;
/******/
/******/ 	// define getter function for harmony exports
/******/ 	__webpack_require__.d = function(exports, name, getter) {
/******/ 		if(!__webpack_require__.o(exports, name)) {
/******/ 			Object.defineProperty(exports, name, {
/******/ 				configurable: false,
/******/ 				enumerable: true,
/******/ 				get: getter
/******/ 			});
/******/ 		}
/******/ 	};
/******/
/******/ 	// getDefaultExport function for compatibility with non-harmony modules
/******/ 	__webpack_require__.n = function(module) {
/******/ 		var getter = module && module.__esModule ?
/******/ 			function getDefault() { return module['default']; } :
/******/ 			function getModuleExports() { return module; };
/******/ 		__webpack_require__.d(getter, 'a', getter);
/******/ 		return getter;
/******/ 	};
/******/
/******/ 	// Object.prototype.hasOwnProperty.call
/******/ 	__webpack_require__.o = function(object, property) { return Object.prototype.hasOwnProperty.call(object, property); };
/******/
/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "";
/******/
/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(__webpack_require__.s = 2);
/******/ })
/************************************************************************/
/******/ ([
/* 0 */
/***/ (function(module, exports, __webpack_require__) {

// Make use of EventEmitter afterwards
const ProgressBar = __webpack_require__(1);

/**
 * Instance of Demo Class to call the Functions related 
 * to increasing the progress
 */
function Demo() {
    // Right now the ProgressBar is created for us, 
    // in future, there can be a dedicated demo to do this for us
    this.progressBar = new ProgressBar({
        selector: '.bar-goes-here',
        width: '190px',
        height: '6px',
        barColors: rainbowBarColors,
    });

    this.progressBar.setProgress(2.39213);
    
    this.progressLogValue = document.querySelector('.progress .value');
    this.completeLogValue = document.querySelector('.complete .value');

    /**
     * Controls for the progress within the JavaScript Application
     */
    this.progressControls = document.querySelectorAll('.progress-controls .control');

    // Attaching the change progress listener to all of the controls
    this.progressControls.forEach(element => {
        element.addEventListener('click', this.changeProgress.bind(this));        
    });

    // Listening for logs emitted from the event emitter with ProgressBar
    this.progressBar.on('updateprogress', this.progressListener.bind(this));
    this.progressBar.on('complete', this.completeListener.bind(this));

    // Calling for the first time with initial progress
    this.progressListener(this.progressBar.progress);
}

/*
 * Sets the progress in percentage
 */
Demo.prototype.progressListener = function (progress) {
    this.progressLogValue.textContent = progress + '%';
}

Demo.prototype.completeListener = function () {
    this.completeLogValue.textContent = 'Yes indeed';
}

/**
 * Sets the Value of complete Log
 */
Demo.prototype.setCompleteLog = function (event) {

}

/**
 * 
 */
Demo.prototype.progressEventHandler = function progressEventHandler() {

}

/**
 * Change progress of the function based on the click
 */
Demo.prototype.changeProgress = function (event) {
    event.preventDefault();
    const targetElement = event.target;
    this.progressBar.setProgress(parseInt(targetElement.textContent, 10));
}

/**
 * Custom function to display different barColors on different 
 * progress that's happening
 */
function rainbowBarColors(progress) {
    const DEFAULT_SPOTIFY_COLOR = '#1db954';
    const SECONDARY_COLOR = '#292929'

    if (progress === 0 || progress === 100) {
        return [ DEFAULT_SPOTIFY_COLOR, SECONDARY_COLOR];
    }

    const rainbowColor = "hsl(" + (360 * progress / 100) + ",80%,50%)"
    return [ rainbowColor, SECONDARY_COLOR ];
}


function initialLoad() {
    const demoInstance = new Demo();
    window.pb = demoInstance.progressBar;
}

initialLoad();

/***/ }),
/* 1 */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
/**
 * Making APIs more extensible by letting the API consumer 
 * extend the interfaces 
 * for own purpose.
 * 
 * Doing it by using callbacks at first within a ProgressBar
 * 
 * ProgressBar that contains an argument map
 * containing the following options
 * - width
 * - height
 * - selector
 * 
 * Displays a progress bar and based on the progress of the progress bar also allows
 * us to set the color of the bar
 */
const mitt = __webpack_require__(7).default;

const DEFAULT_PROGRESS_WIDTH = '90px';
const DEFAULT_PROGRESS_HEIGHT = '4px';

const DEFAULT_PRIMARY_COLOR = '#1db954';
const DEFAULT_SECONDARY_COLOR = '#292929';

const INITIAL_CONTAINER_STYLES = {};
const INITIAL_PROGRESS_BAR_FOREGROUND = {};
const INITIAL_PROGRESS_BAR_BACKGROUND = {};

function defaultBarColors() {
    return [ DEFAULT_PRIMARY_COLOR, DEFAULT_SECONDARY_COLOR ];
}

/**
 * Returns a valid selector for the passed argument
 * based on certain validations
 */
function getValidSelector(selector) {
    if (typeof selector === 'string') {
        const elem = document.querySelector(selector);
        if (!elem) {
            throw new Error('No element for the given `selector`', selector);
        }
        return elem;
    } else if (selector instanceof HTMLElement) {
        return selector;
    } else {
        throw new Error('`selector` is not of valid type', selector);
    }
}

function ProgressBar(options) {
    /*
     * Validation not important since defaults are there
     */
    this.width = options.width || DEFAULT_PROGRESS_WIDTH;
    this.height = options.height || DEFAULT_PROGRESS_HEIGHT;
    
    // this._emitter = mitt();
    // this.on   = this._emitter.on;
    // this.off  = this._emitter.off;
    // this.emit = this._emitter.emit;

    /**
     * Progress in percentage
     */
    this.progress = options.initialProgress || 0;

    /**
     * Callback for letting the developers change the color of par dynamiclly based on progress
     * 
     * function with barColor, accept a percentage and provide
     * an array of two colors primary and secondary
     */
    if (options.barColors) {
        /**
         * Method is private and is inaccessible from the outside
         */
        this._barColors = options.barColors;
    } else {
        this._barColors = defaultBarColors;
    }

    this.selector = getValidSelector(options.selector);

    this._attachEmitter();
    this.create();
}

/**
 * Method `create` creates the progress bar with the elements
 * that are required
 */
ProgressBar.prototype.create = function create() {
    const container = document.createElement('div');
    container.classList.add(['progress-bar']);

    /**
     * Container for the Progress Bar
     * Separate elements are found in the future using (querySelector)s
     */
    container.innerHTML = `
        <div class="progress-bar__bg">
            <div class="progress-bar__fg"></div>
        </div>
    `;

    const progressBarBg = container.querySelector('div.progress-bar__bg');
    const progressBarFg = container.querySelector('div.progress-bar__fg');

    /**
     * Add default styling here for the progress bar
     * to make them look beautiful when they first
     * appear on our front end
     */
    this.container = container;
    this.progressBarBg = progressBarBg;
    this.progressBarFg = progressBarFg;

    // Initialization to display the progress bar appropriately
    this._setSize();
    this.show(true);

    /** Initializes the progress specified or the default one */
    this.setProgress(this.progress);

    this.selector.appendChild(this.container);
}

/**
 * Attaches the EventEmitter to the progress bar
 */
ProgressBar.prototype._attachEmitter = function _attachEmitter() {
    const emitter = this.emitter = mitt();

    // Assigning all the emitter properties to emitter
    Object.assign(this, emitter);
}

/**
 * Sets the progress for ProgressBar
 */
ProgressBar.prototype.setProgress = function setProgress(progress) {
    this.progress = progress > 100 ? 100 : progress < 0 ? 0 : progress;
    this.emitProgressEvents();

    this.progressBarFg.style.width = progress + '%';
    this.setProgressColors();
}

ProgressBar.prototype.emitProgressEvents = function progressEvents() {
    if (this.progress >= 100) { this.emit('complete'); }
    this.emit('updateprogress', this.progress);
}

/**
 * Sets the color for the progress bar
 */
ProgressBar.prototype.setProgressColors = function _setProgressColors() {
    const [ fgColor, bgColor ] = this._barColors(this.progress);
    
    this.progressBarFg.style.backgroundColor = fgColor;
    this.progressBarBg.style.backgroundColor = bgColor;
}

ProgressBar.prototype._setSize = function _setSize() {
    this.container.style.width = this.width;
    this.container.style.height = this.height;
}

/**
 * Toggles the display of the ProgressBar
 */
ProgressBar.prototype.show = function show (isVisible) {
    this.container.style.display = isVisible ? '' : 'none';
}

module.exports = ProgressBar;


/***/ }),
/* 2 */
/***/ (function(module, exports, __webpack_require__) {

__webpack_require__(0);
__webpack_require__(0);
__webpack_require__(1);
module.exports = __webpack_require__(4);


/***/ }),
/* 3 */,
/* 4 */
/***/ (function(module, exports, __webpack_require__) {

/* WEBPACK VAR INJECTION */(function(__dirname) {const path = __webpack_require__(5);

module.exports = {
    entry: './index.js',
    output: {
        filename: 'bundle.js',
        path: path.resolve(__dirname, 'dist')
    }
}
/* WEBPACK VAR INJECTION */}.call(exports, "/"))

/***/ }),
/* 5 */
/***/ (function(module, exports, __webpack_require__) {

/* WEBPACK VAR INJECTION */(function(process) {// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

// resolves . and .. elements in a path array with directory names there
// must be no slashes, empty elements, or device names (c:\) in the array
// (so also no leading and trailing slashes - it does not distinguish
// relative and absolute paths)
function normalizeArray(parts, allowAboveRoot) {
  // if the path tries to go above the root, `up` ends up > 0
  var up = 0;
  for (var i = parts.length - 1; i >= 0; i--) {
    var last = parts[i];
    if (last === '.') {
      parts.splice(i, 1);
    } else if (last === '..') {
      parts.splice(i, 1);
      up++;
    } else if (up) {
      parts.splice(i, 1);
      up--;
    }
  }

  // if the path is allowed to go above the root, restore leading ..s
  if (allowAboveRoot) {
    for (; up--; up) {
      parts.unshift('..');
    }
  }

  return parts;
}

// Split a filename into [root, dir, basename, ext], unix version
// 'root' is just a slash, or nothing.
var splitPathRe =
    /^(\/?|)([\s\S]*?)((?:\.{1,2}|[^\/]+?|)(\.[^.\/]*|))(?:[\/]*)$/;
var splitPath = function(filename) {
  return splitPathRe.exec(filename).slice(1);
};

// path.resolve([from ...], to)
// posix version
exports.resolve = function() {
  var resolvedPath = '',
      resolvedAbsolute = false;

  for (var i = arguments.length - 1; i >= -1 && !resolvedAbsolute; i--) {
    var path = (i >= 0) ? arguments[i] : process.cwd();

    // Skip empty and invalid entries
    if (typeof path !== 'string') {
      throw new TypeError('Arguments to path.resolve must be strings');
    } else if (!path) {
      continue;
    }

    resolvedPath = path + '/' + resolvedPath;
    resolvedAbsolute = path.charAt(0) === '/';
  }

  // At this point the path should be resolved to a full absolute path, but
  // handle relative paths to be safe (might happen when process.cwd() fails)

  // Normalize the path
  resolvedPath = normalizeArray(filter(resolvedPath.split('/'), function(p) {
    return !!p;
  }), !resolvedAbsolute).join('/');

  return ((resolvedAbsolute ? '/' : '') + resolvedPath) || '.';
};

// path.normalize(path)
// posix version
exports.normalize = function(path) {
  var isAbsolute = exports.isAbsolute(path),
      trailingSlash = substr(path, -1) === '/';

  // Normalize the path
  path = normalizeArray(filter(path.split('/'), function(p) {
    return !!p;
  }), !isAbsolute).join('/');

  if (!path && !isAbsolute) {
    path = '.';
  }
  if (path && trailingSlash) {
    path += '/';
  }

  return (isAbsolute ? '/' : '') + path;
};

// posix version
exports.isAbsolute = function(path) {
  return path.charAt(0) === '/';
};

// posix version
exports.join = function() {
  var paths = Array.prototype.slice.call(arguments, 0);
  return exports.normalize(filter(paths, function(p, index) {
    if (typeof p !== 'string') {
      throw new TypeError('Arguments to path.join must be strings');
    }
    return p;
  }).join('/'));
};


// path.relative(from, to)
// posix version
exports.relative = function(from, to) {
  from = exports.resolve(from).substr(1);
  to = exports.resolve(to).substr(1);

  function trim(arr) {
    var start = 0;
    for (; start < arr.length; start++) {
      if (arr[start] !== '') break;
    }

    var end = arr.length - 1;
    for (; end >= 0; end--) {
      if (arr[end] !== '') break;
    }

    if (start > end) return [];
    return arr.slice(start, end - start + 1);
  }

  var fromParts = trim(from.split('/'));
  var toParts = trim(to.split('/'));

  var length = Math.min(fromParts.length, toParts.length);
  var samePartsLength = length;
  for (var i = 0; i < length; i++) {
    if (fromParts[i] !== toParts[i]) {
      samePartsLength = i;
      break;
    }
  }

  var outputParts = [];
  for (var i = samePartsLength; i < fromParts.length; i++) {
    outputParts.push('..');
  }

  outputParts = outputParts.concat(toParts.slice(samePartsLength));

  return outputParts.join('/');
};

exports.sep = '/';
exports.delimiter = ':';

exports.dirname = function(path) {
  var result = splitPath(path),
      root = result[0],
      dir = result[1];

  if (!root && !dir) {
    // No dirname whatsoever
    return '.';
  }

  if (dir) {
    // It has a dirname, strip trailing slash
    dir = dir.substr(0, dir.length - 1);
  }

  return root + dir;
};


exports.basename = function(path, ext) {
  var f = splitPath(path)[2];
  // TODO: make this comparison case-insensitive on windows?
  if (ext && f.substr(-1 * ext.length) === ext) {
    f = f.substr(0, f.length - ext.length);
  }
  return f;
};


exports.extname = function(path) {
  return splitPath(path)[3];
};

function filter (xs, f) {
    if (xs.filter) return xs.filter(f);
    var res = [];
    for (var i = 0; i < xs.length; i++) {
        if (f(xs[i], i, xs)) res.push(xs[i]);
    }
    return res;
}

// String.prototype.substr - negative index don't work in IE8
var substr = 'ab'.substr(-1) === 'b'
    ? function (str, start, len) { return str.substr(start, len) }
    : function (str, start, len) {
        if (start < 0) start = str.length + start;
        return str.substr(start, len);
    }
;

/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(6)))

/***/ }),
/* 6 */
/***/ (function(module, exports) {

// shim for using process in browser
var process = module.exports = {};

// cached from whatever global is present so that test runners that stub it
// don't break things.  But we need to wrap it in a try catch in case it is
// wrapped in strict mode code which doesn't define any globals.  It's inside a
// function because try/catches deoptimize in certain engines.

var cachedSetTimeout;
var cachedClearTimeout;

function defaultSetTimout() {
    throw new Error('setTimeout has not been defined');
}
function defaultClearTimeout () {
    throw new Error('clearTimeout has not been defined');
}
(function () {
    try {
        if (typeof setTimeout === 'function') {
            cachedSetTimeout = setTimeout;
        } else {
            cachedSetTimeout = defaultSetTimout;
        }
    } catch (e) {
        cachedSetTimeout = defaultSetTimout;
    }
    try {
        if (typeof clearTimeout === 'function') {
            cachedClearTimeout = clearTimeout;
        } else {
            cachedClearTimeout = defaultClearTimeout;
        }
    } catch (e) {
        cachedClearTimeout = defaultClearTimeout;
    }
} ())
function runTimeout(fun) {
    if (cachedSetTimeout === setTimeout) {
        //normal enviroments in sane situations
        return setTimeout(fun, 0);
    }
    // if setTimeout wasn't available but was latter defined
    if ((cachedSetTimeout === defaultSetTimout || !cachedSetTimeout) && setTimeout) {
        cachedSetTimeout = setTimeout;
        return setTimeout(fun, 0);
    }
    try {
        // when when somebody has screwed with setTimeout but no I.E. maddness
        return cachedSetTimeout(fun, 0);
    } catch(e){
        try {
            // When we are in I.E. but the script has been evaled so I.E. doesn't trust the global object when called normally
            return cachedSetTimeout.call(null, fun, 0);
        } catch(e){
            // same as above but when it's a version of I.E. that must have the global object for 'this', hopfully our context correct otherwise it will throw a global error
            return cachedSetTimeout.call(this, fun, 0);
        }
    }


}
function runClearTimeout(marker) {
    if (cachedClearTimeout === clearTimeout) {
        //normal enviroments in sane situations
        return clearTimeout(marker);
    }
    // if clearTimeout wasn't available but was latter defined
    if ((cachedClearTimeout === defaultClearTimeout || !cachedClearTimeout) && clearTimeout) {
        cachedClearTimeout = clearTimeout;
        return clearTimeout(marker);
    }
    try {
        // when when somebody has screwed with setTimeout but no I.E. maddness
        return cachedClearTimeout(marker);
    } catch (e){
        try {
            // When we are in I.E. but the script has been evaled so I.E. doesn't  trust the global object when called normally
            return cachedClearTimeout.call(null, marker);
        } catch (e){
            // same as above but when it's a version of I.E. that must have the global object for 'this', hopfully our context correct otherwise it will throw a global error.
            // Some versions of I.E. have different rules for clearTimeout vs setTimeout
            return cachedClearTimeout.call(this, marker);
        }
    }



}
var queue = [];
var draining = false;
var currentQueue;
var queueIndex = -1;

function cleanUpNextTick() {
    if (!draining || !currentQueue) {
        return;
    }
    draining = false;
    if (currentQueue.length) {
        queue = currentQueue.concat(queue);
    } else {
        queueIndex = -1;
    }
    if (queue.length) {
        drainQueue();
    }
}

function drainQueue() {
    if (draining) {
        return;
    }
    var timeout = runTimeout(cleanUpNextTick);
    draining = true;

    var len = queue.length;
    while(len) {
        currentQueue = queue;
        queue = [];
        while (++queueIndex < len) {
            if (currentQueue) {
                currentQueue[queueIndex].run();
            }
        }
        queueIndex = -1;
        len = queue.length;
    }
    currentQueue = null;
    draining = false;
    runClearTimeout(timeout);
}

process.nextTick = function (fun) {
    var args = new Array(arguments.length - 1);
    if (arguments.length > 1) {
        for (var i = 1; i < arguments.length; i++) {
            args[i - 1] = arguments[i];
        }
    }
    queue.push(new Item(fun, args));
    if (queue.length === 1 && !draining) {
        runTimeout(drainQueue);
    }
};

// v8 likes predictible objects
function Item(fun, array) {
    this.fun = fun;
    this.array = array;
}
Item.prototype.run = function () {
    this.fun.apply(null, this.array);
};
process.title = 'browser';
process.browser = true;
process.env = {};
process.argv = [];
process.version = ''; // empty string to avoid regexp issues
process.versions = {};

function noop() {}

process.on = noop;
process.addListener = noop;
process.once = noop;
process.off = noop;
process.removeListener = noop;
process.removeAllListeners = noop;
process.emit = noop;
process.prependListener = noop;
process.prependOnceListener = noop;

process.listeners = function (name) { return [] }

process.binding = function (name) {
    throw new Error('process.binding is not supported');
};

process.cwd = function () { return '/' };
process.chdir = function (dir) {
    throw new Error('process.chdir is not supported');
};
process.umask = function() { return 0; };


/***/ }),
/* 7 */
/***/ (function(module, __webpack_exports__, __webpack_require__) {

"use strict";
Object.defineProperty(__webpack_exports__, "__esModule", { value: true });
//      
// An event handler can take an optional event argument
// and should not return a value
                                          
// An array of all currently registered event handlers for a type
                                            
// A map of event types and their corresponding event handlers.
                        
                                   
  

/** Mitt: Tiny (~200b) functional event emitter / pubsub.
 *  @name mitt
 *  @returns {Mitt}
 */
function mitt(all                 ) {
	all = all || Object.create(null);

	return {
		/**
		 * Register an event handler for the given type.
		 *
		 * @param  {String} type	Type of event to listen for, or `"*"` for all events
		 * @param  {Function} handler Function to call in response to given event
		 * @memberOf mitt
		 */
		on: function on(type        , handler              ) {
			(all[type] || (all[type] = [])).push(handler);
		},

		/**
		 * Remove an event handler for the given type.
		 *
		 * @param  {String} type	Type of event to unregister `handler` from, or `"*"`
		 * @param  {Function} handler Handler function to remove
		 * @memberOf mitt
		 */
		off: function off(type        , handler              ) {
			if (all[type]) {
				all[type].splice(all[type].indexOf(handler) >>> 0, 1);
			}
		},

		/**
		 * Invoke all handlers for the given type.
		 * If present, `"*"` handlers are invoked after type-matched handlers.
		 *
		 * @param {String} type  The event type to invoke
		 * @param {Any} [evt]  Any value (object is recommended and powerful), passed to each handler
		 * @memberof mitt
		 */
		emit: function emit(type        , evt     ) {
			(all[type] || []).map(function (handler) { handler(evt); });
			(all['*'] || []).map(function (handler) { handler(type, evt); });
		}
	};
}

/* harmony default export */ __webpack_exports__["default"] = (mitt);
//# sourceMappingURL=mitt.es.js.map


/***/ })
/******/ ]);
/******/ (function(modules) { // webpackBootstrap
/******/ 	// The module cache
/******/ 	var installedModules = {};

/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {

/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId])
/******/ 			return installedModules[moduleId].exports;

/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			exports: {},
/******/ 			id: moduleId,
/******/ 			loaded: false
/******/ 		};

/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);

/******/ 		// Flag the module as loaded
/******/ 		module.loaded = true;

/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}


/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;

/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;

/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "";

/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(0);
/******/ })
/************************************************************************/
/******/ ([
/* 0 */
/***/ function(module, exports, __webpack_require__) {

	'use strict';

	var _jquery = __webpack_require__(1);

	var _jquery2 = _interopRequireDefault(_jquery);

	var _bootstrapMin = __webpack_require__(2);

	var _bootstrapMin2 = _interopRequireDefault(_bootstrapMin);

	var _gakHeader = __webpack_require__(11);

	var _gakHeader2 = _interopRequireDefault(_gakHeader);

	var _gakFooter = __webpack_require__(13);

	var _gakFooter2 = _interopRequireDefault(_gakFooter);

	var _mustache = __webpack_require__(15);

	var _mustache2 = _interopRequireDefault(_mustache);

	var _videos = __webpack_require__(16);

	var _videos2 = _interopRequireDefault(_videos);

	var _home = __webpack_require__(21);

	var _home2 = _interopRequireDefault(_home);

	var _home3 = __webpack_require__(26);

	var _home4 = _interopRequireDefault(_home3);

	var _videos3 = __webpack_require__(31);

	var _videos4 = _interopRequireDefault(_videos3);

	var _info = __webpack_require__(32);

	var _info2 = _interopRequireDefault(_info);

	var _contact = __webpack_require__(34);

	var _contact2 = _interopRequireDefault(_contact);

	function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

	(0, _jquery2.default)(document).ready(function () {
		//add links for navbar

		clickLink(_home4.default).done(function () {
			_home2.default.loadNews();
		});

		(0, _jquery2.default)('#nav-home').click(function (event) {
			event.preventDefault();
			(0, _jquery2.default)('.active').removeClass();
			(0, _jquery2.default)('#nav-home').addClass('active');
			clickLink(_home4.default).done(function () {
				_home2.default.loadNews();
			});
		});

		(0, _jquery2.default)('#nav-video').click(function (event) {
			event.preventDefault();
			(0, _jquery2.default)('.active').removeClass();
			(0, _jquery2.default)('#nav-video').addClass('active');
			clickLink(_videos4.default).done(function () {
				_videos2.default.loadVids();
			});
		});

		(0, _jquery2.default)('#nav-info').click(function (event) {
			event.preventDefault();
			(0, _jquery2.default)('.active').removeClass();
			(0, _jquery2.default)('#nav-info').addClass('active');
			clickLink(_info2.default);
		});

		(0, _jquery2.default)('#nav-contact').click(function (event) {
			event.preventDefault();
			(0, _jquery2.default)('.active').removeClass();
			(0, _jquery2.default)('#nav-contact').addClass('active');
			clickLink(_contact2.default);
		});
	});

	function clickLink(template) {
		return (0, _jquery2.default)('#mainArea').html(_mustache2.default.render(template)).promise();
	}

/***/ },
/* 1 */
/***/ function(module, exports, __webpack_require__) {

	var __WEBPACK_AMD_DEFINE_ARRAY__, __WEBPACK_AMD_DEFINE_RESULT__;/*eslint-disable no-unused-vars*/
	/*!
	 * jQuery JavaScript Library v3.1.0
	 * https://jquery.com/
	 *
	 * Includes Sizzle.js
	 * https://sizzlejs.com/
	 *
	 * Copyright jQuery Foundation and other contributors
	 * Released under the MIT license
	 * https://jquery.org/license
	 *
	 * Date: 2016-07-07T21:44Z
	 */
	( function( global, factory ) {

		"use strict";

		if ( typeof module === "object" && typeof module.exports === "object" ) {

			// For CommonJS and CommonJS-like environments where a proper `window`
			// is present, execute the factory and get jQuery.
			// For environments that do not have a `window` with a `document`
			// (such as Node.js), expose a factory as module.exports.
			// This accentuates the need for the creation of a real `window`.
			// e.g. var jQuery = require("jquery")(window);
			// See ticket #14549 for more info.
			module.exports = global.document ?
				factory( global, true ) :
				function( w ) {
					if ( !w.document ) {
						throw new Error( "jQuery requires a window with a document" );
					}
					return factory( w );
				};
		} else {
			factory( global );
		}

	// Pass this if window is not defined yet
	} )( typeof window !== "undefined" ? window : this, function( window, noGlobal ) {

	// Edge <= 12 - 13+, Firefox <=18 - 45+, IE 10 - 11, Safari 5.1 - 9+, iOS 6 - 9.1
	// throw exceptions when non-strict code (e.g., ASP.NET 4.5) accesses strict mode
	// arguments.callee.caller (trac-13335). But as of jQuery 3.0 (2016), strict mode should be common
	// enough that all such attempts are guarded in a try block.
	"use strict";

	var arr = [];

	var document = window.document;

	var getProto = Object.getPrototypeOf;

	var slice = arr.slice;

	var concat = arr.concat;

	var push = arr.push;

	var indexOf = arr.indexOf;

	var class2type = {};

	var toString = class2type.toString;

	var hasOwn = class2type.hasOwnProperty;

	var fnToString = hasOwn.toString;

	var ObjectFunctionString = fnToString.call( Object );

	var support = {};



		function DOMEval( code, doc ) {
			doc = doc || document;

			var script = doc.createElement( "script" );

			script.text = code;
			doc.head.appendChild( script ).parentNode.removeChild( script );
		}
	/* global Symbol */
	// Defining this global in .eslintrc would create a danger of using the global
	// unguarded in another place, it seems safer to define global only for this module



	var
		version = "3.1.0",

		// Define a local copy of jQuery
		jQuery = function( selector, context ) {

			// The jQuery object is actually just the init constructor 'enhanced'
			// Need init if jQuery is called (just allow error to be thrown if not included)
			return new jQuery.fn.init( selector, context );
		},

		// Support: Android <=4.0 only
		// Make sure we trim BOM and NBSP
		rtrim = /^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g,

		// Matches dashed string for camelizing
		rmsPrefix = /^-ms-/,
		rdashAlpha = /-([a-z])/g,

		// Used by jQuery.camelCase as callback to replace()
		fcamelCase = function( all, letter ) {
			return letter.toUpperCase();
		};

	jQuery.fn = jQuery.prototype = {

		// The current version of jQuery being used
		jquery: version,

		constructor: jQuery,

		// The default length of a jQuery object is 0
		length: 0,

		toArray: function() {
			return slice.call( this );
		},

		// Get the Nth element in the matched element set OR
		// Get the whole matched element set as a clean array
		get: function( num ) {
			return num != null ?

				// Return just the one element from the set
				( num < 0 ? this[ num + this.length ] : this[ num ] ) :

				// Return all the elements in a clean array
				slice.call( this );
		},

		// Take an array of elements and push it onto the stack
		// (returning the new matched element set)
		pushStack: function( elems ) {

			// Build a new jQuery matched element set
			var ret = jQuery.merge( this.constructor(), elems );

			// Add the old object onto the stack (as a reference)
			ret.prevObject = this;

			// Return the newly-formed element set
			return ret;
		},

		// Execute a callback for every element in the matched set.
		each: function( callback ) {
			return jQuery.each( this, callback );
		},

		map: function( callback ) {
			return this.pushStack( jQuery.map( this, function( elem, i ) {
				return callback.call( elem, i, elem );
			} ) );
		},

		slice: function() {
			return this.pushStack( slice.apply( this, arguments ) );
		},

		first: function() {
			return this.eq( 0 );
		},

		last: function() {
			return this.eq( -1 );
		},

		eq: function( i ) {
			var len = this.length,
				j = +i + ( i < 0 ? len : 0 );
			return this.pushStack( j >= 0 && j < len ? [ this[ j ] ] : [] );
		},

		end: function() {
			return this.prevObject || this.constructor();
		},

		// For internal use only.
		// Behaves like an Array's method, not like a jQuery method.
		push: push,
		sort: arr.sort,
		splice: arr.splice
	};

	jQuery.extend = jQuery.fn.extend = function() {
		var options, name, src, copy, copyIsArray, clone,
			target = arguments[ 0 ] || {},
			i = 1,
			length = arguments.length,
			deep = false;

		// Handle a deep copy situation
		if ( typeof target === "boolean" ) {
			deep = target;

			// Skip the boolean and the target
			target = arguments[ i ] || {};
			i++;
		}

		// Handle case when target is a string or something (possible in deep copy)
		if ( typeof target !== "object" && !jQuery.isFunction( target ) ) {
			target = {};
		}

		// Extend jQuery itself if only one argument is passed
		if ( i === length ) {
			target = this;
			i--;
		}

		for ( ; i < length; i++ ) {

			// Only deal with non-null/undefined values
			if ( ( options = arguments[ i ] ) != null ) {

				// Extend the base object
				for ( name in options ) {
					src = target[ name ];
					copy = options[ name ];

					// Prevent never-ending loop
					if ( target === copy ) {
						continue;
					}

					// Recurse if we're merging plain objects or arrays
					if ( deep && copy && ( jQuery.isPlainObject( copy ) ||
						( copyIsArray = jQuery.isArray( copy ) ) ) ) {

						if ( copyIsArray ) {
							copyIsArray = false;
							clone = src && jQuery.isArray( src ) ? src : [];

						} else {
							clone = src && jQuery.isPlainObject( src ) ? src : {};
						}

						// Never move original objects, clone them
						target[ name ] = jQuery.extend( deep, clone, copy );

					// Don't bring in undefined values
					} else if ( copy !== undefined ) {
						target[ name ] = copy;
					}
				}
			}
		}

		// Return the modified object
		return target;
	};

	jQuery.extend( {

		// Unique for each copy of jQuery on the page
		expando: "jQuery" + ( version + Math.random() ).replace( /\D/g, "" ),

		// Assume jQuery is ready without the ready module
		isReady: true,

		error: function( msg ) {
			throw new Error( msg );
		},

		noop: function() {},

		isFunction: function( obj ) {
			return jQuery.type( obj ) === "function";
		},

		isArray: Array.isArray,

		isWindow: function( obj ) {
			return obj != null && obj === obj.window;
		},

		isNumeric: function( obj ) {

			// As of jQuery 3.0, isNumeric is limited to
			// strings and numbers (primitives or objects)
			// that can be coerced to finite numbers (gh-2662)
			var type = jQuery.type( obj );
			return ( type === "number" || type === "string" ) &&

				// parseFloat NaNs numeric-cast false positives ("")
				// ...but misinterprets leading-number strings, particularly hex literals ("0x...")
				// subtraction forces infinities to NaN
				!isNaN( obj - parseFloat( obj ) );
		},

		isPlainObject: function( obj ) {
			var proto, Ctor;

			// Detect obvious negatives
			// Use toString instead of jQuery.type to catch host objects
			if ( !obj || toString.call( obj ) !== "[object Object]" ) {
				return false;
			}

			proto = getProto( obj );

			// Objects with no prototype (e.g., `Object.create( null )`) are plain
			if ( !proto ) {
				return true;
			}

			// Objects with prototype are plain iff they were constructed by a global Object function
			Ctor = hasOwn.call( proto, "constructor" ) && proto.constructor;
			return typeof Ctor === "function" && fnToString.call( Ctor ) === ObjectFunctionString;
		},

		isEmptyObject: function( obj ) {

			/* eslint-disable no-unused-vars */
			// See https://github.com/eslint/eslint/issues/6125
			var name;

			for ( name in obj ) {
				return false;
			}
			return true;
		},

		type: function( obj ) {
			if ( obj == null ) {
				return obj + "";
			}

			// Support: Android <=2.3 only (functionish RegExp)
			return typeof obj === "object" || typeof obj === "function" ?
				class2type[ toString.call( obj ) ] || "object" :
				typeof obj;
		},

		// Evaluates a script in a global context
		globalEval: function( code ) {
			DOMEval( code );
		},

		// Convert dashed to camelCase; used by the css and data modules
		// Support: IE <=9 - 11, Edge 12 - 13
		// Microsoft forgot to hump their vendor prefix (#9572)
		camelCase: function( string ) {
			return string.replace( rmsPrefix, "ms-" ).replace( rdashAlpha, fcamelCase );
		},

		nodeName: function( elem, name ) {
			return elem.nodeName && elem.nodeName.toLowerCase() === name.toLowerCase();
		},

		each: function( obj, callback ) {
			var length, i = 0;

			if ( isArrayLike( obj ) ) {
				length = obj.length;
				for ( ; i < length; i++ ) {
					if ( callback.call( obj[ i ], i, obj[ i ] ) === false ) {
						break;
					}
				}
			} else {
				for ( i in obj ) {
					if ( callback.call( obj[ i ], i, obj[ i ] ) === false ) {
						break;
					}
				}
			}

			return obj;
		},

		// Support: Android <=4.0 only
		trim: function( text ) {
			return text == null ?
				"" :
				( text + "" ).replace( rtrim, "" );
		},

		// results is for internal usage only
		makeArray: function( arr, results ) {
			var ret = results || [];

			if ( arr != null ) {
				if ( isArrayLike( Object( arr ) ) ) {
					jQuery.merge( ret,
						typeof arr === "string" ?
						[ arr ] : arr
					);
				} else {
					push.call( ret, arr );
				}
			}

			return ret;
		},

		inArray: function( elem, arr, i ) {
			return arr == null ? -1 : indexOf.call( arr, elem, i );
		},

		// Support: Android <=4.0 only, PhantomJS 1 only
		// push.apply(_, arraylike) throws on ancient WebKit
		merge: function( first, second ) {
			var len = +second.length,
				j = 0,
				i = first.length;

			for ( ; j < len; j++ ) {
				first[ i++ ] = second[ j ];
			}

			first.length = i;

			return first;
		},

		grep: function( elems, callback, invert ) {
			var callbackInverse,
				matches = [],
				i = 0,
				length = elems.length,
				callbackExpect = !invert;

			// Go through the array, only saving the items
			// that pass the validator function
			for ( ; i < length; i++ ) {
				callbackInverse = !callback( elems[ i ], i );
				if ( callbackInverse !== callbackExpect ) {
					matches.push( elems[ i ] );
				}
			}

			return matches;
		},

		// arg is for internal usage only
		map: function( elems, callback, arg ) {
			var length, value,
				i = 0,
				ret = [];

			// Go through the array, translating each of the items to their new values
			if ( isArrayLike( elems ) ) {
				length = elems.length;
				for ( ; i < length; i++ ) {
					value = callback( elems[ i ], i, arg );

					if ( value != null ) {
						ret.push( value );
					}
				}

			// Go through every key on the object,
			} else {
				for ( i in elems ) {
					value = callback( elems[ i ], i, arg );

					if ( value != null ) {
						ret.push( value );
					}
				}
			}

			// Flatten any nested arrays
			return concat.apply( [], ret );
		},

		// A global GUID counter for objects
		guid: 1,

		// Bind a function to a context, optionally partially applying any
		// arguments.
		proxy: function( fn, context ) {
			var tmp, args, proxy;

			if ( typeof context === "string" ) {
				tmp = fn[ context ];
				context = fn;
				fn = tmp;
			}

			// Quick check to determine if target is callable, in the spec
			// this throws a TypeError, but we will just return undefined.
			if ( !jQuery.isFunction( fn ) ) {
				return undefined;
			}

			// Simulated bind
			args = slice.call( arguments, 2 );
			proxy = function() {
				return fn.apply( context || this, args.concat( slice.call( arguments ) ) );
			};

			// Set the guid of unique handler to the same of original handler, so it can be removed
			proxy.guid = fn.guid = fn.guid || jQuery.guid++;

			return proxy;
		},

		now: Date.now,

		// jQuery.support is not used in Core but other projects attach their
		// properties to it so it needs to exist.
		support: support
	} );

	if ( typeof Symbol === "function" ) {
		jQuery.fn[ Symbol.iterator ] = arr[ Symbol.iterator ];
	}

	// Populate the class2type map
	jQuery.each( "Boolean Number String Function Array Date RegExp Object Error Symbol".split( " " ),
	function( i, name ) {
		class2type[ "[object " + name + "]" ] = name.toLowerCase();
	} );

	function isArrayLike( obj ) {

		// Support: real iOS 8.2 only (not reproducible in simulator)
		// `in` check used to prevent JIT error (gh-2145)
		// hasOwn isn't used here due to false negatives
		// regarding Nodelist length in IE
		var length = !!obj && "length" in obj && obj.length,
			type = jQuery.type( obj );

		if ( type === "function" || jQuery.isWindow( obj ) ) {
			return false;
		}

		return type === "array" || length === 0 ||
			typeof length === "number" && length > 0 && ( length - 1 ) in obj;
	}
	var Sizzle =
	/*!
	 * Sizzle CSS Selector Engine v2.3.0
	 * https://sizzlejs.com/
	 *
	 * Copyright jQuery Foundation and other contributors
	 * Released under the MIT license
	 * http://jquery.org/license
	 *
	 * Date: 2016-01-04
	 */
	(function( window ) {

	var i,
		support,
		Expr,
		getText,
		isXML,
		tokenize,
		compile,
		select,
		outermostContext,
		sortInput,
		hasDuplicate,

		// Local document vars
		setDocument,
		document,
		docElem,
		documentIsHTML,
		rbuggyQSA,
		rbuggyMatches,
		matches,
		contains,

		// Instance-specific data
		expando = "sizzle" + 1 * new Date(),
		preferredDoc = window.document,
		dirruns = 0,
		done = 0,
		classCache = createCache(),
		tokenCache = createCache(),
		compilerCache = createCache(),
		sortOrder = function( a, b ) {
			if ( a === b ) {
				hasDuplicate = true;
			}
			return 0;
		},

		// Instance methods
		hasOwn = ({}).hasOwnProperty,
		arr = [],
		pop = arr.pop,
		push_native = arr.push,
		push = arr.push,
		slice = arr.slice,
		// Use a stripped-down indexOf as it's faster than native
		// https://jsperf.com/thor-indexof-vs-for/5
		indexOf = function( list, elem ) {
			var i = 0,
				len = list.length;
			for ( ; i < len; i++ ) {
				if ( list[i] === elem ) {
					return i;
				}
			}
			return -1;
		},

		booleans = "checked|selected|async|autofocus|autoplay|controls|defer|disabled|hidden|ismap|loop|multiple|open|readonly|required|scoped",

		// Regular expressions

		// http://www.w3.org/TR/css3-selectors/#whitespace
		whitespace = "[\\x20\\t\\r\\n\\f]",

		// http://www.w3.org/TR/CSS21/syndata.html#value-def-identifier
		identifier = "(?:\\\\.|[\\w-]|[^\0-\\xa0])+",

		// Attribute selectors: http://www.w3.org/TR/selectors/#attribute-selectors
		attributes = "\\[" + whitespace + "*(" + identifier + ")(?:" + whitespace +
			// Operator (capture 2)
			"*([*^$|!~]?=)" + whitespace +
			// "Attribute values must be CSS identifiers [capture 5] or strings [capture 3 or capture 4]"
			"*(?:'((?:\\\\.|[^\\\\'])*)'|\"((?:\\\\.|[^\\\\\"])*)\"|(" + identifier + "))|)" + whitespace +
			"*\\]",

		pseudos = ":(" + identifier + ")(?:\\((" +
			// To reduce the number of selectors needing tokenize in the preFilter, prefer arguments:
			// 1. quoted (capture 3; capture 4 or capture 5)
			"('((?:\\\\.|[^\\\\'])*)'|\"((?:\\\\.|[^\\\\\"])*)\")|" +
			// 2. simple (capture 6)
			"((?:\\\\.|[^\\\\()[\\]]|" + attributes + ")*)|" +
			// 3. anything else (capture 2)
			".*" +
			")\\)|)",

		// Leading and non-escaped trailing whitespace, capturing some non-whitespace characters preceding the latter
		rwhitespace = new RegExp( whitespace + "+", "g" ),
		rtrim = new RegExp( "^" + whitespace + "+|((?:^|[^\\\\])(?:\\\\.)*)" + whitespace + "+$", "g" ),

		rcomma = new RegExp( "^" + whitespace + "*," + whitespace + "*" ),
		rcombinators = new RegExp( "^" + whitespace + "*([>+~]|" + whitespace + ")" + whitespace + "*" ),

		rattributeQuotes = new RegExp( "=" + whitespace + "*([^\\]'\"]*?)" + whitespace + "*\\]", "g" ),

		rpseudo = new RegExp( pseudos ),
		ridentifier = new RegExp( "^" + identifier + "$" ),

		matchExpr = {
			"ID": new RegExp( "^#(" + identifier + ")" ),
			"CLASS": new RegExp( "^\\.(" + identifier + ")" ),
			"TAG": new RegExp( "^(" + identifier + "|[*])" ),
			"ATTR": new RegExp( "^" + attributes ),
			"PSEUDO": new RegExp( "^" + pseudos ),
			"CHILD": new RegExp( "^:(only|first|last|nth|nth-last)-(child|of-type)(?:\\(" + whitespace +
				"*(even|odd|(([+-]|)(\\d*)n|)" + whitespace + "*(?:([+-]|)" + whitespace +
				"*(\\d+)|))" + whitespace + "*\\)|)", "i" ),
			"bool": new RegExp( "^(?:" + booleans + ")$", "i" ),
			// For use in libraries implementing .is()
			// We use this for POS matching in `select`
			"needsContext": new RegExp( "^" + whitespace + "*[>+~]|:(even|odd|eq|gt|lt|nth|first|last)(?:\\(" +
				whitespace + "*((?:-\\d)?\\d*)" + whitespace + "*\\)|)(?=[^-]|$)", "i" )
		},

		rinputs = /^(?:input|select|textarea|button)$/i,
		rheader = /^h\d$/i,

		rnative = /^[^{]+\{\s*\[native \w/,

		// Easily-parseable/retrievable ID or TAG or CLASS selectors
		rquickExpr = /^(?:#([\w-]+)|(\w+)|\.([\w-]+))$/,

		rsibling = /[+~]/,

		// CSS escapes
		// http://www.w3.org/TR/CSS21/syndata.html#escaped-characters
		runescape = new RegExp( "\\\\([\\da-f]{1,6}" + whitespace + "?|(" + whitespace + ")|.)", "ig" ),
		funescape = function( _, escaped, escapedWhitespace ) {
			var high = "0x" + escaped - 0x10000;
			// NaN means non-codepoint
			// Support: Firefox<24
			// Workaround erroneous numeric interpretation of +"0x"
			return high !== high || escapedWhitespace ?
				escaped :
				high < 0 ?
					// BMP codepoint
					String.fromCharCode( high + 0x10000 ) :
					// Supplemental Plane codepoint (surrogate pair)
					String.fromCharCode( high >> 10 | 0xD800, high & 0x3FF | 0xDC00 );
		},

		// CSS string/identifier serialization
		// https://drafts.csswg.org/cssom/#common-serializing-idioms
		rcssescape = /([\0-\x1f\x7f]|^-?\d)|^-$|[^\x80-\uFFFF\w-]/g,
		fcssescape = function( ch, asCodePoint ) {
			if ( asCodePoint ) {

				// U+0000 NULL becomes U+FFFD REPLACEMENT CHARACTER
				if ( ch === "\0" ) {
					return "\uFFFD";
				}

				// Control characters and (dependent upon position) numbers get escaped as code points
				return ch.slice( 0, -1 ) + "\\" + ch.charCodeAt( ch.length - 1 ).toString( 16 ) + " ";
			}

			// Other potentially-special ASCII characters get backslash-escaped
			return "\\" + ch;
		},

		// Used for iframes
		// See setDocument()
		// Removing the function wrapper causes a "Permission Denied"
		// error in IE
		unloadHandler = function() {
			setDocument();
		},

		disabledAncestor = addCombinator(
			function( elem ) {
				return elem.disabled === true;
			},
			{ dir: "parentNode", next: "legend" }
		);

	// Optimize for push.apply( _, NodeList )
	try {
		push.apply(
			(arr = slice.call( preferredDoc.childNodes )),
			preferredDoc.childNodes
		);
		// Support: Android<4.0
		// Detect silently failing push.apply
		arr[ preferredDoc.childNodes.length ].nodeType;
	} catch ( e ) {
		push = { apply: arr.length ?

			// Leverage slice if possible
			function( target, els ) {
				push_native.apply( target, slice.call(els) );
			} :

			// Support: IE<9
			// Otherwise append directly
			function( target, els ) {
				var j = target.length,
					i = 0;
				// Can't trust NodeList.length
				while ( (target[j++] = els[i++]) ) {}
				target.length = j - 1;
			}
		};
	}

	function Sizzle( selector, context, results, seed ) {
		var m, i, elem, nid, match, groups, newSelector,
			newContext = context && context.ownerDocument,

			// nodeType defaults to 9, since context defaults to document
			nodeType = context ? context.nodeType : 9;

		results = results || [];

		// Return early from calls with invalid selector or context
		if ( typeof selector !== "string" || !selector ||
			nodeType !== 1 && nodeType !== 9 && nodeType !== 11 ) {

			return results;
		}

		// Try to shortcut find operations (as opposed to filters) in HTML documents
		if ( !seed ) {

			if ( ( context ? context.ownerDocument || context : preferredDoc ) !== document ) {
				setDocument( context );
			}
			context = context || document;

			if ( documentIsHTML ) {

				// If the selector is sufficiently simple, try using a "get*By*" DOM method
				// (excepting DocumentFragment context, where the methods don't exist)
				if ( nodeType !== 11 && (match = rquickExpr.exec( selector )) ) {

					// ID selector
					if ( (m = match[1]) ) {

						// Document context
						if ( nodeType === 9 ) {
							if ( (elem = context.getElementById( m )) ) {

								// Support: IE, Opera, Webkit
								// TODO: identify versions
								// getElementById can match elements by name instead of ID
								if ( elem.id === m ) {
									results.push( elem );
									return results;
								}
							} else {
								return results;
							}

						// Element context
						} else {

							// Support: IE, Opera, Webkit
							// TODO: identify versions
							// getElementById can match elements by name instead of ID
							if ( newContext && (elem = newContext.getElementById( m )) &&
								contains( context, elem ) &&
								elem.id === m ) {

								results.push( elem );
								return results;
							}
						}

					// Type selector
					} else if ( match[2] ) {
						push.apply( results, context.getElementsByTagName( selector ) );
						return results;

					// Class selector
					} else if ( (m = match[3]) && support.getElementsByClassName &&
						context.getElementsByClassName ) {

						push.apply( results, context.getElementsByClassName( m ) );
						return results;
					}
				}

				// Take advantage of querySelectorAll
				if ( support.qsa &&
					!compilerCache[ selector + " " ] &&
					(!rbuggyQSA || !rbuggyQSA.test( selector )) ) {

					if ( nodeType !== 1 ) {
						newContext = context;
						newSelector = selector;

					// qSA looks outside Element context, which is not what we want
					// Thanks to Andrew Dupont for this workaround technique
					// Support: IE <=8
					// Exclude object elements
					} else if ( context.nodeName.toLowerCase() !== "object" ) {

						// Capture the context ID, setting it first if necessary
						if ( (nid = context.getAttribute( "id" )) ) {
							nid = nid.replace( rcssescape, fcssescape );
						} else {
							context.setAttribute( "id", (nid = expando) );
						}

						// Prefix every selector in the list
						groups = tokenize( selector );
						i = groups.length;
						while ( i-- ) {
							groups[i] = "#" + nid + " " + toSelector( groups[i] );
						}
						newSelector = groups.join( "," );

						// Expand context for sibling selectors
						newContext = rsibling.test( selector ) && testContext( context.parentNode ) ||
							context;
					}

					if ( newSelector ) {
						try {
							push.apply( results,
								newContext.querySelectorAll( newSelector )
							);
							return results;
						} catch ( qsaError ) {
						} finally {
							if ( nid === expando ) {
								context.removeAttribute( "id" );
							}
						}
					}
				}
			}
		}

		// All others
		return select( selector.replace( rtrim, "$1" ), context, results, seed );
	}

	/**
	 * Create key-value caches of limited size
	 * @returns {function(string, object)} Returns the Object data after storing it on itself with
	 *	property name the (space-suffixed) string and (if the cache is larger than Expr.cacheLength)
	 *	deleting the oldest entry
	 */
	function createCache() {
		var keys = [];

		function cache( key, value ) {
			// Use (key + " ") to avoid collision with native prototype properties (see Issue #157)
			if ( keys.push( key + " " ) > Expr.cacheLength ) {
				// Only keep the most recent entries
				delete cache[ keys.shift() ];
			}
			return (cache[ key + " " ] = value);
		}
		return cache;
	}

	/**
	 * Mark a function for special use by Sizzle
	 * @param {Function} fn The function to mark
	 */
	function markFunction( fn ) {
		fn[ expando ] = true;
		return fn;
	}

	/**
	 * Support testing using an element
	 * @param {Function} fn Passed the created element and returns a boolean result
	 */
	function assert( fn ) {
		var el = document.createElement("fieldset");

		try {
			return !!fn( el );
		} catch (e) {
			return false;
		} finally {
			// Remove from its parent by default
			if ( el.parentNode ) {
				el.parentNode.removeChild( el );
			}
			// release memory in IE
			el = null;
		}
	}

	/**
	 * Adds the same handler for all of the specified attrs
	 * @param {String} attrs Pipe-separated list of attributes
	 * @param {Function} handler The method that will be applied
	 */
	function addHandle( attrs, handler ) {
		var arr = attrs.split("|"),
			i = arr.length;

		while ( i-- ) {
			Expr.attrHandle[ arr[i] ] = handler;
		}
	}

	/**
	 * Checks document order of two siblings
	 * @param {Element} a
	 * @param {Element} b
	 * @returns {Number} Returns less than 0 if a precedes b, greater than 0 if a follows b
	 */
	function siblingCheck( a, b ) {
		var cur = b && a,
			diff = cur && a.nodeType === 1 && b.nodeType === 1 &&
				a.sourceIndex - b.sourceIndex;

		// Use IE sourceIndex if available on both nodes
		if ( diff ) {
			return diff;
		}

		// Check if b follows a
		if ( cur ) {
			while ( (cur = cur.nextSibling) ) {
				if ( cur === b ) {
					return -1;
				}
			}
		}

		return a ? 1 : -1;
	}

	/**
	 * Returns a function to use in pseudos for input types
	 * @param {String} type
	 */
	function createInputPseudo( type ) {
		return function( elem ) {
			var name = elem.nodeName.toLowerCase();
			return name === "input" && elem.type === type;
		};
	}

	/**
	 * Returns a function to use in pseudos for buttons
	 * @param {String} type
	 */
	function createButtonPseudo( type ) {
		return function( elem ) {
			var name = elem.nodeName.toLowerCase();
			return (name === "input" || name === "button") && elem.type === type;
		};
	}

	/**
	 * Returns a function to use in pseudos for :enabled/:disabled
	 * @param {Boolean} disabled true for :disabled; false for :enabled
	 */
	function createDisabledPseudo( disabled ) {
		// Known :disabled false positives:
		// IE: *[disabled]:not(button, input, select, textarea, optgroup, option, menuitem, fieldset)
		// not IE: fieldset[disabled] > legend:nth-of-type(n+2) :can-disable
		return function( elem ) {

			// Check form elements and option elements for explicit disabling
			return "label" in elem && elem.disabled === disabled ||
				"form" in elem && elem.disabled === disabled ||

				// Check non-disabled form elements for fieldset[disabled] ancestors
				"form" in elem && elem.disabled === false && (
					// Support: IE6-11+
					// Ancestry is covered for us
					elem.isDisabled === disabled ||

					// Otherwise, assume any non-<option> under fieldset[disabled] is disabled
					/* jshint -W018 */
					elem.isDisabled !== !disabled &&
						("label" in elem || !disabledAncestor( elem )) !== disabled
				);
		};
	}

	/**
	 * Returns a function to use in pseudos for positionals
	 * @param {Function} fn
	 */
	function createPositionalPseudo( fn ) {
		return markFunction(function( argument ) {
			argument = +argument;
			return markFunction(function( seed, matches ) {
				var j,
					matchIndexes = fn( [], seed.length, argument ),
					i = matchIndexes.length;

				// Match elements found at the specified indexes
				while ( i-- ) {
					if ( seed[ (j = matchIndexes[i]) ] ) {
						seed[j] = !(matches[j] = seed[j]);
					}
				}
			});
		});
	}

	/**
	 * Checks a node for validity as a Sizzle context
	 * @param {Element|Object=} context
	 * @returns {Element|Object|Boolean} The input node if acceptable, otherwise a falsy value
	 */
	function testContext( context ) {
		return context && typeof context.getElementsByTagName !== "undefined" && context;
	}

	// Expose support vars for convenience
	support = Sizzle.support = {};

	/**
	 * Detects XML nodes
	 * @param {Element|Object} elem An element or a document
	 * @returns {Boolean} True iff elem is a non-HTML XML node
	 */
	isXML = Sizzle.isXML = function( elem ) {
		// documentElement is verified for cases where it doesn't yet exist
		// (such as loading iframes in IE - #4833)
		var documentElement = elem && (elem.ownerDocument || elem).documentElement;
		return documentElement ? documentElement.nodeName !== "HTML" : false;
	};

	/**
	 * Sets document-related variables once based on the current document
	 * @param {Element|Object} [doc] An element or document object to use to set the document
	 * @returns {Object} Returns the current document
	 */
	setDocument = Sizzle.setDocument = function( node ) {
		var hasCompare, subWindow,
			doc = node ? node.ownerDocument || node : preferredDoc;

		// Return early if doc is invalid or already selected
		if ( doc === document || doc.nodeType !== 9 || !doc.documentElement ) {
			return document;
		}

		// Update global variables
		document = doc;
		docElem = document.documentElement;
		documentIsHTML = !isXML( document );

		// Support: IE 9-11, Edge
		// Accessing iframe documents after unload throws "permission denied" errors (jQuery #13936)
		if ( preferredDoc !== document &&
			(subWindow = document.defaultView) && subWindow.top !== subWindow ) {

			// Support: IE 11, Edge
			if ( subWindow.addEventListener ) {
				subWindow.addEventListener( "unload", unloadHandler, false );

			// Support: IE 9 - 10 only
			} else if ( subWindow.attachEvent ) {
				subWindow.attachEvent( "onunload", unloadHandler );
			}
		}

		/* Attributes
		---------------------------------------------------------------------- */

		// Support: IE<8
		// Verify that getAttribute really returns attributes and not properties
		// (excepting IE8 booleans)
		support.attributes = assert(function( el ) {
			el.className = "i";
			return !el.getAttribute("className");
		});

		/* getElement(s)By*
		---------------------------------------------------------------------- */

		// Check if getElementsByTagName("*") returns only elements
		support.getElementsByTagName = assert(function( el ) {
			el.appendChild( document.createComment("") );
			return !el.getElementsByTagName("*").length;
		});

		// Support: IE<9
		support.getElementsByClassName = rnative.test( document.getElementsByClassName );

		// Support: IE<10
		// Check if getElementById returns elements by name
		// The broken getElementById methods don't pick up programmatically-set names,
		// so use a roundabout getElementsByName test
		support.getById = assert(function( el ) {
			docElem.appendChild( el ).id = expando;
			return !document.getElementsByName || !document.getElementsByName( expando ).length;
		});

		// ID find and filter
		if ( support.getById ) {
			Expr.find["ID"] = function( id, context ) {
				if ( typeof context.getElementById !== "undefined" && documentIsHTML ) {
					var m = context.getElementById( id );
					return m ? [ m ] : [];
				}
			};
			Expr.filter["ID"] = function( id ) {
				var attrId = id.replace( runescape, funescape );
				return function( elem ) {
					return elem.getAttribute("id") === attrId;
				};
			};
		} else {
			// Support: IE6/7
			// getElementById is not reliable as a find shortcut
			delete Expr.find["ID"];

			Expr.filter["ID"] =  function( id ) {
				var attrId = id.replace( runescape, funescape );
				return function( elem ) {
					var node = typeof elem.getAttributeNode !== "undefined" &&
						elem.getAttributeNode("id");
					return node && node.value === attrId;
				};
			};
		}

		// Tag
		Expr.find["TAG"] = support.getElementsByTagName ?
			function( tag, context ) {
				if ( typeof context.getElementsByTagName !== "undefined" ) {
					return context.getElementsByTagName( tag );

				// DocumentFragment nodes don't have gEBTN
				} else if ( support.qsa ) {
					return context.querySelectorAll( tag );
				}
			} :

			function( tag, context ) {
				var elem,
					tmp = [],
					i = 0,
					// By happy coincidence, a (broken) gEBTN appears on DocumentFragment nodes too
					results = context.getElementsByTagName( tag );

				// Filter out possible comments
				if ( tag === "*" ) {
					while ( (elem = results[i++]) ) {
						if ( elem.nodeType === 1 ) {
							tmp.push( elem );
						}
					}

					return tmp;
				}
				return results;
			};

		// Class
		Expr.find["CLASS"] = support.getElementsByClassName && function( className, context ) {
			if ( typeof context.getElementsByClassName !== "undefined" && documentIsHTML ) {
				return context.getElementsByClassName( className );
			}
		};

		/* QSA/matchesSelector
		---------------------------------------------------------------------- */

		// QSA and matchesSelector support

		// matchesSelector(:active) reports false when true (IE9/Opera 11.5)
		rbuggyMatches = [];

		// qSa(:focus) reports false when true (Chrome 21)
		// We allow this because of a bug in IE8/9 that throws an error
		// whenever `document.activeElement` is accessed on an iframe
		// So, we allow :focus to pass through QSA all the time to avoid the IE error
		// See https://bugs.jquery.com/ticket/13378
		rbuggyQSA = [];

		if ( (support.qsa = rnative.test( document.querySelectorAll )) ) {
			// Build QSA regex
			// Regex strategy adopted from Diego Perini
			assert(function( el ) {
				// Select is set to empty string on purpose
				// This is to test IE's treatment of not explicitly
				// setting a boolean content attribute,
				// since its presence should be enough
				// https://bugs.jquery.com/ticket/12359
				docElem.appendChild( el ).innerHTML = "<a id='" + expando + "'></a>" +
					"<select id='" + expando + "-\r\\' msallowcapture=''>" +
					"<option selected=''></option></select>";

				// Support: IE8, Opera 11-12.16
				// Nothing should be selected when empty strings follow ^= or $= or *=
				// The test attribute must be unknown in Opera but "safe" for WinRT
				// https://msdn.microsoft.com/en-us/library/ie/hh465388.aspx#attribute_section
				if ( el.querySelectorAll("[msallowcapture^='']").length ) {
					rbuggyQSA.push( "[*^$]=" + whitespace + "*(?:''|\"\")" );
				}

				// Support: IE8
				// Boolean attributes and "value" are not treated correctly
				if ( !el.querySelectorAll("[selected]").length ) {
					rbuggyQSA.push( "\\[" + whitespace + "*(?:value|" + booleans + ")" );
				}

				// Support: Chrome<29, Android<4.4, Safari<7.0+, iOS<7.0+, PhantomJS<1.9.8+
				if ( !el.querySelectorAll( "[id~=" + expando + "-]" ).length ) {
					rbuggyQSA.push("~=");
				}

				// Webkit/Opera - :checked should return selected option elements
				// http://www.w3.org/TR/2011/REC-css3-selectors-20110929/#checked
				// IE8 throws error here and will not see later tests
				if ( !el.querySelectorAll(":checked").length ) {
					rbuggyQSA.push(":checked");
				}

				// Support: Safari 8+, iOS 8+
				// https://bugs.webkit.org/show_bug.cgi?id=136851
				// In-page `selector#id sibling-combinator selector` fails
				if ( !el.querySelectorAll( "a#" + expando + "+*" ).length ) {
					rbuggyQSA.push(".#.+[+~]");
				}
			});

			assert(function( el ) {
				el.innerHTML = "<a href='' disabled='disabled'></a>" +
					"<select disabled='disabled'><option/></select>";

				// Support: Windows 8 Native Apps
				// The type and name attributes are restricted during .innerHTML assignment
				var input = document.createElement("input");
				input.setAttribute( "type", "hidden" );
				el.appendChild( input ).setAttribute( "name", "D" );

				// Support: IE8
				// Enforce case-sensitivity of name attribute
				if ( el.querySelectorAll("[name=d]").length ) {
					rbuggyQSA.push( "name" + whitespace + "*[*^$|!~]?=" );
				}

				// FF 3.5 - :enabled/:disabled and hidden elements (hidden elements are still enabled)
				// IE8 throws error here and will not see later tests
				if ( el.querySelectorAll(":enabled").length !== 2 ) {
					rbuggyQSA.push( ":enabled", ":disabled" );
				}

				// Support: IE9-11+
				// IE's :disabled selector does not pick up the children of disabled fieldsets
				docElem.appendChild( el ).disabled = true;
				if ( el.querySelectorAll(":disabled").length !== 2 ) {
					rbuggyQSA.push( ":enabled", ":disabled" );
				}

				// Opera 10-11 does not throw on post-comma invalid pseudos
				el.querySelectorAll("*,:x");
				rbuggyQSA.push(",.*:");
			});
		}

		if ( (support.matchesSelector = rnative.test( (matches = docElem.matches ||
			docElem.webkitMatchesSelector ||
			docElem.mozMatchesSelector ||
			docElem.oMatchesSelector ||
			docElem.msMatchesSelector) )) ) {

			assert(function( el ) {
				// Check to see if it's possible to do matchesSelector
				// on a disconnected node (IE 9)
				support.disconnectedMatch = matches.call( el, "*" );

				// This should fail with an exception
				// Gecko does not error, returns false instead
				matches.call( el, "[s!='']:x" );
				rbuggyMatches.push( "!=", pseudos );
			});
		}

		rbuggyQSA = rbuggyQSA.length && new RegExp( rbuggyQSA.join("|") );
		rbuggyMatches = rbuggyMatches.length && new RegExp( rbuggyMatches.join("|") );

		/* Contains
		---------------------------------------------------------------------- */
		hasCompare = rnative.test( docElem.compareDocumentPosition );

		// Element contains another
		// Purposefully self-exclusive
		// As in, an element does not contain itself
		contains = hasCompare || rnative.test( docElem.contains ) ?
			function( a, b ) {
				var adown = a.nodeType === 9 ? a.documentElement : a,
					bup = b && b.parentNode;
				return a === bup || !!( bup && bup.nodeType === 1 && (
					adown.contains ?
						adown.contains( bup ) :
						a.compareDocumentPosition && a.compareDocumentPosition( bup ) & 16
				));
			} :
			function( a, b ) {
				if ( b ) {
					while ( (b = b.parentNode) ) {
						if ( b === a ) {
							return true;
						}
					}
				}
				return false;
			};

		/* Sorting
		---------------------------------------------------------------------- */

		// Document order sorting
		sortOrder = hasCompare ?
		function( a, b ) {

			// Flag for duplicate removal
			if ( a === b ) {
				hasDuplicate = true;
				return 0;
			}

			// Sort on method existence if only one input has compareDocumentPosition
			var compare = !a.compareDocumentPosition - !b.compareDocumentPosition;
			if ( compare ) {
				return compare;
			}

			// Calculate position if both inputs belong to the same document
			compare = ( a.ownerDocument || a ) === ( b.ownerDocument || b ) ?
				a.compareDocumentPosition( b ) :

				// Otherwise we know they are disconnected
				1;

			// Disconnected nodes
			if ( compare & 1 ||
				(!support.sortDetached && b.compareDocumentPosition( a ) === compare) ) {

				// Choose the first element that is related to our preferred document
				if ( a === document || a.ownerDocument === preferredDoc && contains(preferredDoc, a) ) {
					return -1;
				}
				if ( b === document || b.ownerDocument === preferredDoc && contains(preferredDoc, b) ) {
					return 1;
				}

				// Maintain original order
				return sortInput ?
					( indexOf( sortInput, a ) - indexOf( sortInput, b ) ) :
					0;
			}

			return compare & 4 ? -1 : 1;
		} :
		function( a, b ) {
			// Exit early if the nodes are identical
			if ( a === b ) {
				hasDuplicate = true;
				return 0;
			}

			var cur,
				i = 0,
				aup = a.parentNode,
				bup = b.parentNode,
				ap = [ a ],
				bp = [ b ];

			// Parentless nodes are either documents or disconnected
			if ( !aup || !bup ) {
				return a === document ? -1 :
					b === document ? 1 :
					aup ? -1 :
					bup ? 1 :
					sortInput ?
					( indexOf( sortInput, a ) - indexOf( sortInput, b ) ) :
					0;

			// If the nodes are siblings, we can do a quick check
			} else if ( aup === bup ) {
				return siblingCheck( a, b );
			}

			// Otherwise we need full lists of their ancestors for comparison
			cur = a;
			while ( (cur = cur.parentNode) ) {
				ap.unshift( cur );
			}
			cur = b;
			while ( (cur = cur.parentNode) ) {
				bp.unshift( cur );
			}

			// Walk down the tree looking for a discrepancy
			while ( ap[i] === bp[i] ) {
				i++;
			}

			return i ?
				// Do a sibling check if the nodes have a common ancestor
				siblingCheck( ap[i], bp[i] ) :

				// Otherwise nodes in our document sort first
				ap[i] === preferredDoc ? -1 :
				bp[i] === preferredDoc ? 1 :
				0;
		};

		return document;
	};

	Sizzle.matches = function( expr, elements ) {
		return Sizzle( expr, null, null, elements );
	};

	Sizzle.matchesSelector = function( elem, expr ) {
		// Set document vars if needed
		if ( ( elem.ownerDocument || elem ) !== document ) {
			setDocument( elem );
		}

		// Make sure that attribute selectors are quoted
		expr = expr.replace( rattributeQuotes, "='$1']" );

		if ( support.matchesSelector && documentIsHTML &&
			!compilerCache[ expr + " " ] &&
			( !rbuggyMatches || !rbuggyMatches.test( expr ) ) &&
			( !rbuggyQSA     || !rbuggyQSA.test( expr ) ) ) {

			try {
				var ret = matches.call( elem, expr );

				// IE 9's matchesSelector returns false on disconnected nodes
				if ( ret || support.disconnectedMatch ||
						// As well, disconnected nodes are said to be in a document
						// fragment in IE 9
						elem.document && elem.document.nodeType !== 11 ) {
					return ret;
				}
			} catch (e) {}
		}

		return Sizzle( expr, document, null, [ elem ] ).length > 0;
	};

	Sizzle.contains = function( context, elem ) {
		// Set document vars if needed
		if ( ( context.ownerDocument || context ) !== document ) {
			setDocument( context );
		}
		return contains( context, elem );
	};

	Sizzle.attr = function( elem, name ) {
		// Set document vars if needed
		if ( ( elem.ownerDocument || elem ) !== document ) {
			setDocument( elem );
		}

		var fn = Expr.attrHandle[ name.toLowerCase() ],
			// Don't get fooled by Object.prototype properties (jQuery #13807)
			val = fn && hasOwn.call( Expr.attrHandle, name.toLowerCase() ) ?
				fn( elem, name, !documentIsHTML ) :
				undefined;

		return val !== undefined ?
			val :
			support.attributes || !documentIsHTML ?
				elem.getAttribute( name ) :
				(val = elem.getAttributeNode(name)) && val.specified ?
					val.value :
					null;
	};

	Sizzle.escape = function( sel ) {
		return (sel + "").replace( rcssescape, fcssescape );
	};

	Sizzle.error = function( msg ) {
		throw new Error( "Syntax error, unrecognized expression: " + msg );
	};

	/**
	 * Document sorting and removing duplicates
	 * @param {ArrayLike} results
	 */
	Sizzle.uniqueSort = function( results ) {
		var elem,
			duplicates = [],
			j = 0,
			i = 0;

		// Unless we *know* we can detect duplicates, assume their presence
		hasDuplicate = !support.detectDuplicates;
		sortInput = !support.sortStable && results.slice( 0 );
		results.sort( sortOrder );

		if ( hasDuplicate ) {
			while ( (elem = results[i++]) ) {
				if ( elem === results[ i ] ) {
					j = duplicates.push( i );
				}
			}
			while ( j-- ) {
				results.splice( duplicates[ j ], 1 );
			}
		}

		// Clear input after sorting to release objects
		// See https://github.com/jquery/sizzle/pull/225
		sortInput = null;

		return results;
	};

	/**
	 * Utility function for retrieving the text value of an array of DOM nodes
	 * @param {Array|Element} elem
	 */
	getText = Sizzle.getText = function( elem ) {
		var node,
			ret = "",
			i = 0,
			nodeType = elem.nodeType;

		if ( !nodeType ) {
			// If no nodeType, this is expected to be an array
			while ( (node = elem[i++]) ) {
				// Do not traverse comment nodes
				ret += getText( node );
			}
		} else if ( nodeType === 1 || nodeType === 9 || nodeType === 11 ) {
			// Use textContent for elements
			// innerText usage removed for consistency of new lines (jQuery #11153)
			if ( typeof elem.textContent === "string" ) {
				return elem.textContent;
			} else {
				// Traverse its children
				for ( elem = elem.firstChild; elem; elem = elem.nextSibling ) {
					ret += getText( elem );
				}
			}
		} else if ( nodeType === 3 || nodeType === 4 ) {
			return elem.nodeValue;
		}
		// Do not include comment or processing instruction nodes

		return ret;
	};

	Expr = Sizzle.selectors = {

		// Can be adjusted by the user
		cacheLength: 50,

		createPseudo: markFunction,

		match: matchExpr,

		attrHandle: {},

		find: {},

		relative: {
			">": { dir: "parentNode", first: true },
			" ": { dir: "parentNode" },
			"+": { dir: "previousSibling", first: true },
			"~": { dir: "previousSibling" }
		},

		preFilter: {
			"ATTR": function( match ) {
				match[1] = match[1].replace( runescape, funescape );

				// Move the given value to match[3] whether quoted or unquoted
				match[3] = ( match[3] || match[4] || match[5] || "" ).replace( runescape, funescape );

				if ( match[2] === "~=" ) {
					match[3] = " " + match[3] + " ";
				}

				return match.slice( 0, 4 );
			},

			"CHILD": function( match ) {
				/* matches from matchExpr["CHILD"]
					1 type (only|nth|...)
					2 what (child|of-type)
					3 argument (even|odd|\d*|\d*n([+-]\d+)?|...)
					4 xn-component of xn+y argument ([+-]?\d*n|)
					5 sign of xn-component
					6 x of xn-component
					7 sign of y-component
					8 y of y-component
				*/
				match[1] = match[1].toLowerCase();

				if ( match[1].slice( 0, 3 ) === "nth" ) {
					// nth-* requires argument
					if ( !match[3] ) {
						Sizzle.error( match[0] );
					}

					// numeric x and y parameters for Expr.filter.CHILD
					// remember that false/true cast respectively to 0/1
					match[4] = +( match[4] ? match[5] + (match[6] || 1) : 2 * ( match[3] === "even" || match[3] === "odd" ) );
					match[5] = +( ( match[7] + match[8] ) || match[3] === "odd" );

				// other types prohibit arguments
				} else if ( match[3] ) {
					Sizzle.error( match[0] );
				}

				return match;
			},

			"PSEUDO": function( match ) {
				var excess,
					unquoted = !match[6] && match[2];

				if ( matchExpr["CHILD"].test( match[0] ) ) {
					return null;
				}

				// Accept quoted arguments as-is
				if ( match[3] ) {
					match[2] = match[4] || match[5] || "";

				// Strip excess characters from unquoted arguments
				} else if ( unquoted && rpseudo.test( unquoted ) &&
					// Get excess from tokenize (recursively)
					(excess = tokenize( unquoted, true )) &&
					// advance to the next closing parenthesis
					(excess = unquoted.indexOf( ")", unquoted.length - excess ) - unquoted.length) ) {

					// excess is a negative index
					match[0] = match[0].slice( 0, excess );
					match[2] = unquoted.slice( 0, excess );
				}

				// Return only captures needed by the pseudo filter method (type and argument)
				return match.slice( 0, 3 );
			}
		},

		filter: {

			"TAG": function( nodeNameSelector ) {
				var nodeName = nodeNameSelector.replace( runescape, funescape ).toLowerCase();
				return nodeNameSelector === "*" ?
					function() { return true; } :
					function( elem ) {
						return elem.nodeName && elem.nodeName.toLowerCase() === nodeName;
					};
			},

			"CLASS": function( className ) {
				var pattern = classCache[ className + " " ];

				return pattern ||
					(pattern = new RegExp( "(^|" + whitespace + ")" + className + "(" + whitespace + "|$)" )) &&
					classCache( className, function( elem ) {
						return pattern.test( typeof elem.className === "string" && elem.className || typeof elem.getAttribute !== "undefined" && elem.getAttribute("class") || "" );
					});
			},

			"ATTR": function( name, operator, check ) {
				return function( elem ) {
					var result = Sizzle.attr( elem, name );

					if ( result == null ) {
						return operator === "!=";
					}
					if ( !operator ) {
						return true;
					}

					result += "";

					return operator === "=" ? result === check :
						operator === "!=" ? result !== check :
						operator === "^=" ? check && result.indexOf( check ) === 0 :
						operator === "*=" ? check && result.indexOf( check ) > -1 :
						operator === "$=" ? check && result.slice( -check.length ) === check :
						operator === "~=" ? ( " " + result.replace( rwhitespace, " " ) + " " ).indexOf( check ) > -1 :
						operator === "|=" ? result === check || result.slice( 0, check.length + 1 ) === check + "-" :
						false;
				};
			},

			"CHILD": function( type, what, argument, first, last ) {
				var simple = type.slice( 0, 3 ) !== "nth",
					forward = type.slice( -4 ) !== "last",
					ofType = what === "of-type";

				return first === 1 && last === 0 ?

					// Shortcut for :nth-*(n)
					function( elem ) {
						return !!elem.parentNode;
					} :

					function( elem, context, xml ) {
						var cache, uniqueCache, outerCache, node, nodeIndex, start,
							dir = simple !== forward ? "nextSibling" : "previousSibling",
							parent = elem.parentNode,
							name = ofType && elem.nodeName.toLowerCase(),
							useCache = !xml && !ofType,
							diff = false;

						if ( parent ) {

							// :(first|last|only)-(child|of-type)
							if ( simple ) {
								while ( dir ) {
									node = elem;
									while ( (node = node[ dir ]) ) {
										if ( ofType ?
											node.nodeName.toLowerCase() === name :
											node.nodeType === 1 ) {

											return false;
										}
									}
									// Reverse direction for :only-* (if we haven't yet done so)
									start = dir = type === "only" && !start && "nextSibling";
								}
								return true;
							}

							start = [ forward ? parent.firstChild : parent.lastChild ];

							// non-xml :nth-child(...) stores cache data on `parent`
							if ( forward && useCache ) {

								// Seek `elem` from a previously-cached index

								// ...in a gzip-friendly way
								node = parent;
								outerCache = node[ expando ] || (node[ expando ] = {});

								// Support: IE <9 only
								// Defend against cloned attroperties (jQuery gh-1709)
								uniqueCache = outerCache[ node.uniqueID ] ||
									(outerCache[ node.uniqueID ] = {});

								cache = uniqueCache[ type ] || [];
								nodeIndex = cache[ 0 ] === dirruns && cache[ 1 ];
								diff = nodeIndex && cache[ 2 ];
								node = nodeIndex && parent.childNodes[ nodeIndex ];

								while ( (node = ++nodeIndex && node && node[ dir ] ||

									// Fallback to seeking `elem` from the start
									(diff = nodeIndex = 0) || start.pop()) ) {

									// When found, cache indexes on `parent` and break
									if ( node.nodeType === 1 && ++diff && node === elem ) {
										uniqueCache[ type ] = [ dirruns, nodeIndex, diff ];
										break;
									}
								}

							} else {
								// Use previously-cached element index if available
								if ( useCache ) {
									// ...in a gzip-friendly way
									node = elem;
									outerCache = node[ expando ] || (node[ expando ] = {});

									// Support: IE <9 only
									// Defend against cloned attroperties (jQuery gh-1709)
									uniqueCache = outerCache[ node.uniqueID ] ||
										(outerCache[ node.uniqueID ] = {});

									cache = uniqueCache[ type ] || [];
									nodeIndex = cache[ 0 ] === dirruns && cache[ 1 ];
									diff = nodeIndex;
								}

								// xml :nth-child(...)
								// or :nth-last-child(...) or :nth(-last)?-of-type(...)
								if ( diff === false ) {
									// Use the same loop as above to seek `elem` from the start
									while ( (node = ++nodeIndex && node && node[ dir ] ||
										(diff = nodeIndex = 0) || start.pop()) ) {

										if ( ( ofType ?
											node.nodeName.toLowerCase() === name :
											node.nodeType === 1 ) &&
											++diff ) {

											// Cache the index of each encountered element
											if ( useCache ) {
												outerCache = node[ expando ] || (node[ expando ] = {});

												// Support: IE <9 only
												// Defend against cloned attroperties (jQuery gh-1709)
												uniqueCache = outerCache[ node.uniqueID ] ||
													(outerCache[ node.uniqueID ] = {});

												uniqueCache[ type ] = [ dirruns, diff ];
											}

											if ( node === elem ) {
												break;
											}
										}
									}
								}
							}

							// Incorporate the offset, then check against cycle size
							diff -= last;
							return diff === first || ( diff % first === 0 && diff / first >= 0 );
						}
					};
			},

			"PSEUDO": function( pseudo, argument ) {
				// pseudo-class names are case-insensitive
				// http://www.w3.org/TR/selectors/#pseudo-classes
				// Prioritize by case sensitivity in case custom pseudos are added with uppercase letters
				// Remember that setFilters inherits from pseudos
				var args,
					fn = Expr.pseudos[ pseudo ] || Expr.setFilters[ pseudo.toLowerCase() ] ||
						Sizzle.error( "unsupported pseudo: " + pseudo );

				// The user may use createPseudo to indicate that
				// arguments are needed to create the filter function
				// just as Sizzle does
				if ( fn[ expando ] ) {
					return fn( argument );
				}

				// But maintain support for old signatures
				if ( fn.length > 1 ) {
					args = [ pseudo, pseudo, "", argument ];
					return Expr.setFilters.hasOwnProperty( pseudo.toLowerCase() ) ?
						markFunction(function( seed, matches ) {
							var idx,
								matched = fn( seed, argument ),
								i = matched.length;
							while ( i-- ) {
								idx = indexOf( seed, matched[i] );
								seed[ idx ] = !( matches[ idx ] = matched[i] );
							}
						}) :
						function( elem ) {
							return fn( elem, 0, args );
						};
				}

				return fn;
			}
		},

		pseudos: {
			// Potentially complex pseudos
			"not": markFunction(function( selector ) {
				// Trim the selector passed to compile
				// to avoid treating leading and trailing
				// spaces as combinators
				var input = [],
					results = [],
					matcher = compile( selector.replace( rtrim, "$1" ) );

				return matcher[ expando ] ?
					markFunction(function( seed, matches, context, xml ) {
						var elem,
							unmatched = matcher( seed, null, xml, [] ),
							i = seed.length;

						// Match elements unmatched by `matcher`
						while ( i-- ) {
							if ( (elem = unmatched[i]) ) {
								seed[i] = !(matches[i] = elem);
							}
						}
					}) :
					function( elem, context, xml ) {
						input[0] = elem;
						matcher( input, null, xml, results );
						// Don't keep the element (issue #299)
						input[0] = null;
						return !results.pop();
					};
			}),

			"has": markFunction(function( selector ) {
				return function( elem ) {
					return Sizzle( selector, elem ).length > 0;
				};
			}),

			"contains": markFunction(function( text ) {
				text = text.replace( runescape, funescape );
				return function( elem ) {
					return ( elem.textContent || elem.innerText || getText( elem ) ).indexOf( text ) > -1;
				};
			}),

			// "Whether an element is represented by a :lang() selector
			// is based solely on the element's language value
			// being equal to the identifier C,
			// or beginning with the identifier C immediately followed by "-".
			// The matching of C against the element's language value is performed case-insensitively.
			// The identifier C does not have to be a valid language name."
			// http://www.w3.org/TR/selectors/#lang-pseudo
			"lang": markFunction( function( lang ) {
				// lang value must be a valid identifier
				if ( !ridentifier.test(lang || "") ) {
					Sizzle.error( "unsupported lang: " + lang );
				}
				lang = lang.replace( runescape, funescape ).toLowerCase();
				return function( elem ) {
					var elemLang;
					do {
						if ( (elemLang = documentIsHTML ?
							elem.lang :
							elem.getAttribute("xml:lang") || elem.getAttribute("lang")) ) {

							elemLang = elemLang.toLowerCase();
							return elemLang === lang || elemLang.indexOf( lang + "-" ) === 0;
						}
					} while ( (elem = elem.parentNode) && elem.nodeType === 1 );
					return false;
				};
			}),

			// Miscellaneous
			"target": function( elem ) {
				var hash = window.location && window.location.hash;
				return hash && hash.slice( 1 ) === elem.id;
			},

			"root": function( elem ) {
				return elem === docElem;
			},

			"focus": function( elem ) {
				return elem === document.activeElement && (!document.hasFocus || document.hasFocus()) && !!(elem.type || elem.href || ~elem.tabIndex);
			},

			// Boolean properties
			"enabled": createDisabledPseudo( false ),
			"disabled": createDisabledPseudo( true ),

			"checked": function( elem ) {
				// In CSS3, :checked should return both checked and selected elements
				// http://www.w3.org/TR/2011/REC-css3-selectors-20110929/#checked
				var nodeName = elem.nodeName.toLowerCase();
				return (nodeName === "input" && !!elem.checked) || (nodeName === "option" && !!elem.selected);
			},

			"selected": function( elem ) {
				// Accessing this property makes selected-by-default
				// options in Safari work properly
				if ( elem.parentNode ) {
					elem.parentNode.selectedIndex;
				}

				return elem.selected === true;
			},

			// Contents
			"empty": function( elem ) {
				// http://www.w3.org/TR/selectors/#empty-pseudo
				// :empty is negated by element (1) or content nodes (text: 3; cdata: 4; entity ref: 5),
				//   but not by others (comment: 8; processing instruction: 7; etc.)
				// nodeType < 6 works because attributes (2) do not appear as children
				for ( elem = elem.firstChild; elem; elem = elem.nextSibling ) {
					if ( elem.nodeType < 6 ) {
						return false;
					}
				}
				return true;
			},

			"parent": function( elem ) {
				return !Expr.pseudos["empty"]( elem );
			},

			// Element/input types
			"header": function( elem ) {
				return rheader.test( elem.nodeName );
			},

			"input": function( elem ) {
				return rinputs.test( elem.nodeName );
			},

			"button": function( elem ) {
				var name = elem.nodeName.toLowerCase();
				return name === "input" && elem.type === "button" || name === "button";
			},

			"text": function( elem ) {
				var attr;
				return elem.nodeName.toLowerCase() === "input" &&
					elem.type === "text" &&

					// Support: IE<8
					// New HTML5 attribute values (e.g., "search") appear with elem.type === "text"
					( (attr = elem.getAttribute("type")) == null || attr.toLowerCase() === "text" );
			},

			// Position-in-collection
			"first": createPositionalPseudo(function() {
				return [ 0 ];
			}),

			"last": createPositionalPseudo(function( matchIndexes, length ) {
				return [ length - 1 ];
			}),

			"eq": createPositionalPseudo(function( matchIndexes, length, argument ) {
				return [ argument < 0 ? argument + length : argument ];
			}),

			"even": createPositionalPseudo(function( matchIndexes, length ) {
				var i = 0;
				for ( ; i < length; i += 2 ) {
					matchIndexes.push( i );
				}
				return matchIndexes;
			}),

			"odd": createPositionalPseudo(function( matchIndexes, length ) {
				var i = 1;
				for ( ; i < length; i += 2 ) {
					matchIndexes.push( i );
				}
				return matchIndexes;
			}),

			"lt": createPositionalPseudo(function( matchIndexes, length, argument ) {
				var i = argument < 0 ? argument + length : argument;
				for ( ; --i >= 0; ) {
					matchIndexes.push( i );
				}
				return matchIndexes;
			}),

			"gt": createPositionalPseudo(function( matchIndexes, length, argument ) {
				var i = argument < 0 ? argument + length : argument;
				for ( ; ++i < length; ) {
					matchIndexes.push( i );
				}
				return matchIndexes;
			})
		}
	};

	Expr.pseudos["nth"] = Expr.pseudos["eq"];

	// Add button/input type pseudos
	for ( i in { radio: true, checkbox: true, file: true, password: true, image: true } ) {
		Expr.pseudos[ i ] = createInputPseudo( i );
	}
	for ( i in { submit: true, reset: true } ) {
		Expr.pseudos[ i ] = createButtonPseudo( i );
	}

	// Easy API for creating new setFilters
	function setFilters() {}
	setFilters.prototype = Expr.filters = Expr.pseudos;
	Expr.setFilters = new setFilters();

	tokenize = Sizzle.tokenize = function( selector, parseOnly ) {
		var matched, match, tokens, type,
			soFar, groups, preFilters,
			cached = tokenCache[ selector + " " ];

		if ( cached ) {
			return parseOnly ? 0 : cached.slice( 0 );
		}

		soFar = selector;
		groups = [];
		preFilters = Expr.preFilter;

		while ( soFar ) {

			// Comma and first run
			if ( !matched || (match = rcomma.exec( soFar )) ) {
				if ( match ) {
					// Don't consume trailing commas as valid
					soFar = soFar.slice( match[0].length ) || soFar;
				}
				groups.push( (tokens = []) );
			}

			matched = false;

			// Combinators
			if ( (match = rcombinators.exec( soFar )) ) {
				matched = match.shift();
				tokens.push({
					value: matched,
					// Cast descendant combinators to space
					type: match[0].replace( rtrim, " " )
				});
				soFar = soFar.slice( matched.length );
			}

			// Filters
			for ( type in Expr.filter ) {
				if ( (match = matchExpr[ type ].exec( soFar )) && (!preFilters[ type ] ||
					(match = preFilters[ type ]( match ))) ) {
					matched = match.shift();
					tokens.push({
						value: matched,
						type: type,
						matches: match
					});
					soFar = soFar.slice( matched.length );
				}
			}

			if ( !matched ) {
				break;
			}
		}

		// Return the length of the invalid excess
		// if we're just parsing
		// Otherwise, throw an error or return tokens
		return parseOnly ?
			soFar.length :
			soFar ?
				Sizzle.error( selector ) :
				// Cache the tokens
				tokenCache( selector, groups ).slice( 0 );
	};

	function toSelector( tokens ) {
		var i = 0,
			len = tokens.length,
			selector = "";
		for ( ; i < len; i++ ) {
			selector += tokens[i].value;
		}
		return selector;
	}

	function addCombinator( matcher, combinator, base ) {
		var dir = combinator.dir,
			skip = combinator.next,
			key = skip || dir,
			checkNonElements = base && key === "parentNode",
			doneName = done++;

		return combinator.first ?
			// Check against closest ancestor/preceding element
			function( elem, context, xml ) {
				while ( (elem = elem[ dir ]) ) {
					if ( elem.nodeType === 1 || checkNonElements ) {
						return matcher( elem, context, xml );
					}
				}
			} :

			// Check against all ancestor/preceding elements
			function( elem, context, xml ) {
				var oldCache, uniqueCache, outerCache,
					newCache = [ dirruns, doneName ];

				// We can't set arbitrary data on XML nodes, so they don't benefit from combinator caching
				if ( xml ) {
					while ( (elem = elem[ dir ]) ) {
						if ( elem.nodeType === 1 || checkNonElements ) {
							if ( matcher( elem, context, xml ) ) {
								return true;
							}
						}
					}
				} else {
					while ( (elem = elem[ dir ]) ) {
						if ( elem.nodeType === 1 || checkNonElements ) {
							outerCache = elem[ expando ] || (elem[ expando ] = {});

							// Support: IE <9 only
							// Defend against cloned attroperties (jQuery gh-1709)
							uniqueCache = outerCache[ elem.uniqueID ] || (outerCache[ elem.uniqueID ] = {});

							if ( skip && skip === elem.nodeName.toLowerCase() ) {
								elem = elem[ dir ] || elem;
							} else if ( (oldCache = uniqueCache[ key ]) &&
								oldCache[ 0 ] === dirruns && oldCache[ 1 ] === doneName ) {

								// Assign to newCache so results back-propagate to previous elements
								return (newCache[ 2 ] = oldCache[ 2 ]);
							} else {
								// Reuse newcache so results back-propagate to previous elements
								uniqueCache[ key ] = newCache;

								// A match means we're done; a fail means we have to keep checking
								if ( (newCache[ 2 ] = matcher( elem, context, xml )) ) {
									return true;
								}
							}
						}
					}
				}
			};
	}

	function elementMatcher( matchers ) {
		return matchers.length > 1 ?
			function( elem, context, xml ) {
				var i = matchers.length;
				while ( i-- ) {
					if ( !matchers[i]( elem, context, xml ) ) {
						return false;
					}
				}
				return true;
			} :
			matchers[0];
	}

	function multipleContexts( selector, contexts, results ) {
		var i = 0,
			len = contexts.length;
		for ( ; i < len; i++ ) {
			Sizzle( selector, contexts[i], results );
		}
		return results;
	}

	function condense( unmatched, map, filter, context, xml ) {
		var elem,
			newUnmatched = [],
			i = 0,
			len = unmatched.length,
			mapped = map != null;

		for ( ; i < len; i++ ) {
			if ( (elem = unmatched[i]) ) {
				if ( !filter || filter( elem, context, xml ) ) {
					newUnmatched.push( elem );
					if ( mapped ) {
						map.push( i );
					}
				}
			}
		}

		return newUnmatched;
	}

	function setMatcher( preFilter, selector, matcher, postFilter, postFinder, postSelector ) {
		if ( postFilter && !postFilter[ expando ] ) {
			postFilter = setMatcher( postFilter );
		}
		if ( postFinder && !postFinder[ expando ] ) {
			postFinder = setMatcher( postFinder, postSelector );
		}
		return markFunction(function( seed, results, context, xml ) {
			var temp, i, elem,
				preMap = [],
				postMap = [],
				preexisting = results.length,

				// Get initial elements from seed or context
				elems = seed || multipleContexts( selector || "*", context.nodeType ? [ context ] : context, [] ),

				// Prefilter to get matcher input, preserving a map for seed-results synchronization
				matcherIn = preFilter && ( seed || !selector ) ?
					condense( elems, preMap, preFilter, context, xml ) :
					elems,

				matcherOut = matcher ?
					// If we have a postFinder, or filtered seed, or non-seed postFilter or preexisting results,
					postFinder || ( seed ? preFilter : preexisting || postFilter ) ?

						// ...intermediate processing is necessary
						[] :

						// ...otherwise use results directly
						results :
					matcherIn;

			// Find primary matches
			if ( matcher ) {
				matcher( matcherIn, matcherOut, context, xml );
			}

			// Apply postFilter
			if ( postFilter ) {
				temp = condense( matcherOut, postMap );
				postFilter( temp, [], context, xml );

				// Un-match failing elements by moving them back to matcherIn
				i = temp.length;
				while ( i-- ) {
					if ( (elem = temp[i]) ) {
						matcherOut[ postMap[i] ] = !(matcherIn[ postMap[i] ] = elem);
					}
				}
			}

			if ( seed ) {
				if ( postFinder || preFilter ) {
					if ( postFinder ) {
						// Get the final matcherOut by condensing this intermediate into postFinder contexts
						temp = [];
						i = matcherOut.length;
						while ( i-- ) {
							if ( (elem = matcherOut[i]) ) {
								// Restore matcherIn since elem is not yet a final match
								temp.push( (matcherIn[i] = elem) );
							}
						}
						postFinder( null, (matcherOut = []), temp, xml );
					}

					// Move matched elements from seed to results to keep them synchronized
					i = matcherOut.length;
					while ( i-- ) {
						if ( (elem = matcherOut[i]) &&
							(temp = postFinder ? indexOf( seed, elem ) : preMap[i]) > -1 ) {

							seed[temp] = !(results[temp] = elem);
						}
					}
				}

			// Add elements to results, through postFinder if defined
			} else {
				matcherOut = condense(
					matcherOut === results ?
						matcherOut.splice( preexisting, matcherOut.length ) :
						matcherOut
				);
				if ( postFinder ) {
					postFinder( null, results, matcherOut, xml );
				} else {
					push.apply( results, matcherOut );
				}
			}
		});
	}

	function matcherFromTokens( tokens ) {
		var checkContext, matcher, j,
			len = tokens.length,
			leadingRelative = Expr.relative[ tokens[0].type ],
			implicitRelative = leadingRelative || Expr.relative[" "],
			i = leadingRelative ? 1 : 0,

			// The foundational matcher ensures that elements are reachable from top-level context(s)
			matchContext = addCombinator( function( elem ) {
				return elem === checkContext;
			}, implicitRelative, true ),
			matchAnyContext = addCombinator( function( elem ) {
				return indexOf( checkContext, elem ) > -1;
			}, implicitRelative, true ),
			matchers = [ function( elem, context, xml ) {
				var ret = ( !leadingRelative && ( xml || context !== outermostContext ) ) || (
					(checkContext = context).nodeType ?
						matchContext( elem, context, xml ) :
						matchAnyContext( elem, context, xml ) );
				// Avoid hanging onto element (issue #299)
				checkContext = null;
				return ret;
			} ];

		for ( ; i < len; i++ ) {
			if ( (matcher = Expr.relative[ tokens[i].type ]) ) {
				matchers = [ addCombinator(elementMatcher( matchers ), matcher) ];
			} else {
				matcher = Expr.filter[ tokens[i].type ].apply( null, tokens[i].matches );

				// Return special upon seeing a positional matcher
				if ( matcher[ expando ] ) {
					// Find the next relative operator (if any) for proper handling
					j = ++i;
					for ( ; j < len; j++ ) {
						if ( Expr.relative[ tokens[j].type ] ) {
							break;
						}
					}
					return setMatcher(
						i > 1 && elementMatcher( matchers ),
						i > 1 && toSelector(
							// If the preceding token was a descendant combinator, insert an implicit any-element `*`
							tokens.slice( 0, i - 1 ).concat({ value: tokens[ i - 2 ].type === " " ? "*" : "" })
						).replace( rtrim, "$1" ),
						matcher,
						i < j && matcherFromTokens( tokens.slice( i, j ) ),
						j < len && matcherFromTokens( (tokens = tokens.slice( j )) ),
						j < len && toSelector( tokens )
					);
				}
				matchers.push( matcher );
			}
		}

		return elementMatcher( matchers );
	}

	function matcherFromGroupMatchers( elementMatchers, setMatchers ) {
		var bySet = setMatchers.length > 0,
			byElement = elementMatchers.length > 0,
			superMatcher = function( seed, context, xml, results, outermost ) {
				var elem, j, matcher,
					matchedCount = 0,
					i = "0",
					unmatched = seed && [],
					setMatched = [],
					contextBackup = outermostContext,
					// We must always have either seed elements or outermost context
					elems = seed || byElement && Expr.find["TAG"]( "*", outermost ),
					// Use integer dirruns iff this is the outermost matcher
					dirrunsUnique = (dirruns += contextBackup == null ? 1 : Math.random() || 0.1),
					len = elems.length;

				if ( outermost ) {
					outermostContext = context === document || context || outermost;
				}

				// Add elements passing elementMatchers directly to results
				// Support: IE<9, Safari
				// Tolerate NodeList properties (IE: "length"; Safari: <number>) matching elements by id
				for ( ; i !== len && (elem = elems[i]) != null; i++ ) {
					if ( byElement && elem ) {
						j = 0;
						if ( !context && elem.ownerDocument !== document ) {
							setDocument( elem );
							xml = !documentIsHTML;
						}
						while ( (matcher = elementMatchers[j++]) ) {
							if ( matcher( elem, context || document, xml) ) {
								results.push( elem );
								break;
							}
						}
						if ( outermost ) {
							dirruns = dirrunsUnique;
						}
					}

					// Track unmatched elements for set filters
					if ( bySet ) {
						// They will have gone through all possible matchers
						if ( (elem = !matcher && elem) ) {
							matchedCount--;
						}

						// Lengthen the array for every element, matched or not
						if ( seed ) {
							unmatched.push( elem );
						}
					}
				}

				// `i` is now the count of elements visited above, and adding it to `matchedCount`
				// makes the latter nonnegative.
				matchedCount += i;

				// Apply set filters to unmatched elements
				// NOTE: This can be skipped if there are no unmatched elements (i.e., `matchedCount`
				// equals `i`), unless we didn't visit _any_ elements in the above loop because we have
				// no element matchers and no seed.
				// Incrementing an initially-string "0" `i` allows `i` to remain a string only in that
				// case, which will result in a "00" `matchedCount` that differs from `i` but is also
				// numerically zero.
				if ( bySet && i !== matchedCount ) {
					j = 0;
					while ( (matcher = setMatchers[j++]) ) {
						matcher( unmatched, setMatched, context, xml );
					}

					if ( seed ) {
						// Reintegrate element matches to eliminate the need for sorting
						if ( matchedCount > 0 ) {
							while ( i-- ) {
								if ( !(unmatched[i] || setMatched[i]) ) {
									setMatched[i] = pop.call( results );
								}
							}
						}

						// Discard index placeholder values to get only actual matches
						setMatched = condense( setMatched );
					}

					// Add matches to results
					push.apply( results, setMatched );

					// Seedless set matches succeeding multiple successful matchers stipulate sorting
					if ( outermost && !seed && setMatched.length > 0 &&
						( matchedCount + setMatchers.length ) > 1 ) {

						Sizzle.uniqueSort( results );
					}
				}

				// Override manipulation of globals by nested matchers
				if ( outermost ) {
					dirruns = dirrunsUnique;
					outermostContext = contextBackup;
				}

				return unmatched;
			};

		return bySet ?
			markFunction( superMatcher ) :
			superMatcher;
	}

	compile = Sizzle.compile = function( selector, match /* Internal Use Only */ ) {
		var i,
			setMatchers = [],
			elementMatchers = [],
			cached = compilerCache[ selector + " " ];

		if ( !cached ) {
			// Generate a function of recursive functions that can be used to check each element
			if ( !match ) {
				match = tokenize( selector );
			}
			i = match.length;
			while ( i-- ) {
				cached = matcherFromTokens( match[i] );
				if ( cached[ expando ] ) {
					setMatchers.push( cached );
				} else {
					elementMatchers.push( cached );
				}
			}

			// Cache the compiled function
			cached = compilerCache( selector, matcherFromGroupMatchers( elementMatchers, setMatchers ) );

			// Save selector and tokenization
			cached.selector = selector;
		}
		return cached;
	};

	/**
	 * A low-level selection function that works with Sizzle's compiled
	 *  selector functions
	 * @param {String|Function} selector A selector or a pre-compiled
	 *  selector function built with Sizzle.compile
	 * @param {Element} context
	 * @param {Array} [results]
	 * @param {Array} [seed] A set of elements to match against
	 */
	select = Sizzle.select = function( selector, context, results, seed ) {
		var i, tokens, token, type, find,
			compiled = typeof selector === "function" && selector,
			match = !seed && tokenize( (selector = compiled.selector || selector) );

		results = results || [];

		// Try to minimize operations if there is only one selector in the list and no seed
		// (the latter of which guarantees us context)
		if ( match.length === 1 ) {

			// Reduce context if the leading compound selector is an ID
			tokens = match[0] = match[0].slice( 0 );
			if ( tokens.length > 2 && (token = tokens[0]).type === "ID" &&
					support.getById && context.nodeType === 9 && documentIsHTML &&
					Expr.relative[ tokens[1].type ] ) {

				context = ( Expr.find["ID"]( token.matches[0].replace(runescape, funescape), context ) || [] )[0];
				if ( !context ) {
					return results;

				// Precompiled matchers will still verify ancestry, so step up a level
				} else if ( compiled ) {
					context = context.parentNode;
				}

				selector = selector.slice( tokens.shift().value.length );
			}

			// Fetch a seed set for right-to-left matching
			i = matchExpr["needsContext"].test( selector ) ? 0 : tokens.length;
			while ( i-- ) {
				token = tokens[i];

				// Abort if we hit a combinator
				if ( Expr.relative[ (type = token.type) ] ) {
					break;
				}
				if ( (find = Expr.find[ type ]) ) {
					// Search, expanding context for leading sibling combinators
					if ( (seed = find(
						token.matches[0].replace( runescape, funescape ),
						rsibling.test( tokens[0].type ) && testContext( context.parentNode ) || context
					)) ) {

						// If seed is empty or no tokens remain, we can return early
						tokens.splice( i, 1 );
						selector = seed.length && toSelector( tokens );
						if ( !selector ) {
							push.apply( results, seed );
							return results;
						}

						break;
					}
				}
			}
		}

		// Compile and execute a filtering function if one is not provided
		// Provide `match` to avoid retokenization if we modified the selector above
		( compiled || compile( selector, match ) )(
			seed,
			context,
			!documentIsHTML,
			results,
			!context || rsibling.test( selector ) && testContext( context.parentNode ) || context
		);
		return results;
	};

	// One-time assignments

	// Sort stability
	support.sortStable = expando.split("").sort( sortOrder ).join("") === expando;

	// Support: Chrome 14-35+
	// Always assume duplicates if they aren't passed to the comparison function
	support.detectDuplicates = !!hasDuplicate;

	// Initialize against the default document
	setDocument();

	// Support: Webkit<537.32 - Safari 6.0.3/Chrome 25 (fixed in Chrome 27)
	// Detached nodes confoundingly follow *each other*
	support.sortDetached = assert(function( el ) {
		// Should return 1, but returns 4 (following)
		return el.compareDocumentPosition( document.createElement("fieldset") ) & 1;
	});

	// Support: IE<8
	// Prevent attribute/property "interpolation"
	// https://msdn.microsoft.com/en-us/library/ms536429%28VS.85%29.aspx
	if ( !assert(function( el ) {
		el.innerHTML = "<a href='#'></a>";
		return el.firstChild.getAttribute("href") === "#" ;
	}) ) {
		addHandle( "type|href|height|width", function( elem, name, isXML ) {
			if ( !isXML ) {
				return elem.getAttribute( name, name.toLowerCase() === "type" ? 1 : 2 );
			}
		});
	}

	// Support: IE<9
	// Use defaultValue in place of getAttribute("value")
	if ( !support.attributes || !assert(function( el ) {
		el.innerHTML = "<input/>";
		el.firstChild.setAttribute( "value", "" );
		return el.firstChild.getAttribute( "value" ) === "";
	}) ) {
		addHandle( "value", function( elem, name, isXML ) {
			if ( !isXML && elem.nodeName.toLowerCase() === "input" ) {
				return elem.defaultValue;
			}
		});
	}

	// Support: IE<9
	// Use getAttributeNode to fetch booleans when getAttribute lies
	if ( !assert(function( el ) {
		return el.getAttribute("disabled") == null;
	}) ) {
		addHandle( booleans, function( elem, name, isXML ) {
			var val;
			if ( !isXML ) {
				return elem[ name ] === true ? name.toLowerCase() :
						(val = elem.getAttributeNode( name )) && val.specified ?
						val.value :
					null;
			}
		});
	}

	return Sizzle;

	})( window );



	jQuery.find = Sizzle;
	jQuery.expr = Sizzle.selectors;

	// Deprecated
	jQuery.expr[ ":" ] = jQuery.expr.pseudos;
	jQuery.uniqueSort = jQuery.unique = Sizzle.uniqueSort;
	jQuery.text = Sizzle.getText;
	jQuery.isXMLDoc = Sizzle.isXML;
	jQuery.contains = Sizzle.contains;
	jQuery.escapeSelector = Sizzle.escape;




	var dir = function( elem, dir, until ) {
		var matched = [],
			truncate = until !== undefined;

		while ( ( elem = elem[ dir ] ) && elem.nodeType !== 9 ) {
			if ( elem.nodeType === 1 ) {
				if ( truncate && jQuery( elem ).is( until ) ) {
					break;
				}
				matched.push( elem );
			}
		}
		return matched;
	};


	var siblings = function( n, elem ) {
		var matched = [];

		for ( ; n; n = n.nextSibling ) {
			if ( n.nodeType === 1 && n !== elem ) {
				matched.push( n );
			}
		}

		return matched;
	};


	var rneedsContext = jQuery.expr.match.needsContext;

	var rsingleTag = ( /^<([a-z][^\/\0>:\x20\t\r\n\f]*)[\x20\t\r\n\f]*\/?>(?:<\/\1>|)$/i );



	var risSimple = /^.[^:#\[\.,]*$/;

	// Implement the identical functionality for filter and not
	function winnow( elements, qualifier, not ) {
		if ( jQuery.isFunction( qualifier ) ) {
			return jQuery.grep( elements, function( elem, i ) {
				return !!qualifier.call( elem, i, elem ) !== not;
			} );

		}

		if ( qualifier.nodeType ) {
			return jQuery.grep( elements, function( elem ) {
				return ( elem === qualifier ) !== not;
			} );

		}

		if ( typeof qualifier === "string" ) {
			if ( risSimple.test( qualifier ) ) {
				return jQuery.filter( qualifier, elements, not );
			}

			qualifier = jQuery.filter( qualifier, elements );
		}

		return jQuery.grep( elements, function( elem ) {
			return ( indexOf.call( qualifier, elem ) > -1 ) !== not && elem.nodeType === 1;
		} );
	}

	jQuery.filter = function( expr, elems, not ) {
		var elem = elems[ 0 ];

		if ( not ) {
			expr = ":not(" + expr + ")";
		}

		return elems.length === 1 && elem.nodeType === 1 ?
			jQuery.find.matchesSelector( elem, expr ) ? [ elem ] : [] :
			jQuery.find.matches( expr, jQuery.grep( elems, function( elem ) {
				return elem.nodeType === 1;
			} ) );
	};

	jQuery.fn.extend( {
		find: function( selector ) {
			var i, ret,
				len = this.length,
				self = this;

			if ( typeof selector !== "string" ) {
				return this.pushStack( jQuery( selector ).filter( function() {
					for ( i = 0; i < len; i++ ) {
						if ( jQuery.contains( self[ i ], this ) ) {
							return true;
						}
					}
				} ) );
			}

			ret = this.pushStack( [] );

			for ( i = 0; i < len; i++ ) {
				jQuery.find( selector, self[ i ], ret );
			}

			return len > 1 ? jQuery.uniqueSort( ret ) : ret;
		},
		filter: function( selector ) {
			return this.pushStack( winnow( this, selector || [], false ) );
		},
		not: function( selector ) {
			return this.pushStack( winnow( this, selector || [], true ) );
		},
		is: function( selector ) {
			return !!winnow(
				this,

				// If this is a positional/relative selector, check membership in the returned set
				// so $("p:first").is("p:last") won't return true for a doc with two "p".
				typeof selector === "string" && rneedsContext.test( selector ) ?
					jQuery( selector ) :
					selector || [],
				false
			).length;
		}
	} );


	// Initialize a jQuery object


	// A central reference to the root jQuery(document)
	var rootjQuery,

		// A simple way to check for HTML strings
		// Prioritize #id over <tag> to avoid XSS via location.hash (#9521)
		// Strict HTML recognition (#11290: must start with <)
		// Shortcut simple #id case for speed
		rquickExpr = /^(?:\s*(<[\w\W]+>)[^>]*|#([\w-]+))$/,

		init = jQuery.fn.init = function( selector, context, root ) {
			var match, elem;

			// HANDLE: $(""), $(null), $(undefined), $(false)
			if ( !selector ) {
				return this;
			}

			// Method init() accepts an alternate rootjQuery
			// so migrate can support jQuery.sub (gh-2101)
			root = root || rootjQuery;

			// Handle HTML strings
			if ( typeof selector === "string" ) {
				if ( selector[ 0 ] === "<" &&
					selector[ selector.length - 1 ] === ">" &&
					selector.length >= 3 ) {

					// Assume that strings that start and end with <> are HTML and skip the regex check
					match = [ null, selector, null ];

				} else {
					match = rquickExpr.exec( selector );
				}

				// Match html or make sure no context is specified for #id
				if ( match && ( match[ 1 ] || !context ) ) {

					// HANDLE: $(html) -> $(array)
					if ( match[ 1 ] ) {
						context = context instanceof jQuery ? context[ 0 ] : context;

						// Option to run scripts is true for back-compat
						// Intentionally let the error be thrown if parseHTML is not present
						jQuery.merge( this, jQuery.parseHTML(
							match[ 1 ],
							context && context.nodeType ? context.ownerDocument || context : document,
							true
						) );

						// HANDLE: $(html, props)
						if ( rsingleTag.test( match[ 1 ] ) && jQuery.isPlainObject( context ) ) {
							for ( match in context ) {

								// Properties of context are called as methods if possible
								if ( jQuery.isFunction( this[ match ] ) ) {
									this[ match ]( context[ match ] );

								// ...and otherwise set as attributes
								} else {
									this.attr( match, context[ match ] );
								}
							}
						}

						return this;

					// HANDLE: $(#id)
					} else {
						elem = document.getElementById( match[ 2 ] );

						if ( elem ) {

							// Inject the element directly into the jQuery object
							this[ 0 ] = elem;
							this.length = 1;
						}
						return this;
					}

				// HANDLE: $(expr, $(...))
				} else if ( !context || context.jquery ) {
					return ( context || root ).find( selector );

				// HANDLE: $(expr, context)
				// (which is just equivalent to: $(context).find(expr)
				} else {
					return this.constructor( context ).find( selector );
				}

			// HANDLE: $(DOMElement)
			} else if ( selector.nodeType ) {
				this[ 0 ] = selector;
				this.length = 1;
				return this;

			// HANDLE: $(function)
			// Shortcut for document ready
			} else if ( jQuery.isFunction( selector ) ) {
				return root.ready !== undefined ?
					root.ready( selector ) :

					// Execute immediately if ready is not present
					selector( jQuery );
			}

			return jQuery.makeArray( selector, this );
		};

	// Give the init function the jQuery prototype for later instantiation
	init.prototype = jQuery.fn;

	// Initialize central reference
	rootjQuery = jQuery( document );


	var rparentsprev = /^(?:parents|prev(?:Until|All))/,

		// Methods guaranteed to produce a unique set when starting from a unique set
		guaranteedUnique = {
			children: true,
			contents: true,
			next: true,
			prev: true
		};

	jQuery.fn.extend( {
		has: function( target ) {
			var targets = jQuery( target, this ),
				l = targets.length;

			return this.filter( function() {
				var i = 0;
				for ( ; i < l; i++ ) {
					if ( jQuery.contains( this, targets[ i ] ) ) {
						return true;
					}
				}
			} );
		},

		closest: function( selectors, context ) {
			var cur,
				i = 0,
				l = this.length,
				matched = [],
				targets = typeof selectors !== "string" && jQuery( selectors );

			// Positional selectors never match, since there's no _selection_ context
			if ( !rneedsContext.test( selectors ) ) {
				for ( ; i < l; i++ ) {
					for ( cur = this[ i ]; cur && cur !== context; cur = cur.parentNode ) {

						// Always skip document fragments
						if ( cur.nodeType < 11 && ( targets ?
							targets.index( cur ) > -1 :

							// Don't pass non-elements to Sizzle
							cur.nodeType === 1 &&
								jQuery.find.matchesSelector( cur, selectors ) ) ) {

							matched.push( cur );
							break;
						}
					}
				}
			}

			return this.pushStack( matched.length > 1 ? jQuery.uniqueSort( matched ) : matched );
		},

		// Determine the position of an element within the set
		index: function( elem ) {

			// No argument, return index in parent
			if ( !elem ) {
				return ( this[ 0 ] && this[ 0 ].parentNode ) ? this.first().prevAll().length : -1;
			}

			// Index in selector
			if ( typeof elem === "string" ) {
				return indexOf.call( jQuery( elem ), this[ 0 ] );
			}

			// Locate the position of the desired element
			return indexOf.call( this,

				// If it receives a jQuery object, the first element is used
				elem.jquery ? elem[ 0 ] : elem
			);
		},

		add: function( selector, context ) {
			return this.pushStack(
				jQuery.uniqueSort(
					jQuery.merge( this.get(), jQuery( selector, context ) )
				)
			);
		},

		addBack: function( selector ) {
			return this.add( selector == null ?
				this.prevObject : this.prevObject.filter( selector )
			);
		}
	} );

	function sibling( cur, dir ) {
		while ( ( cur = cur[ dir ] ) && cur.nodeType !== 1 ) {}
		return cur;
	}

	jQuery.each( {
		parent: function( elem ) {
			var parent = elem.parentNode;
			return parent && parent.nodeType !== 11 ? parent : null;
		},
		parents: function( elem ) {
			return dir( elem, "parentNode" );
		},
		parentsUntil: function( elem, i, until ) {
			return dir( elem, "parentNode", until );
		},
		next: function( elem ) {
			return sibling( elem, "nextSibling" );
		},
		prev: function( elem ) {
			return sibling( elem, "previousSibling" );
		},
		nextAll: function( elem ) {
			return dir( elem, "nextSibling" );
		},
		prevAll: function( elem ) {
			return dir( elem, "previousSibling" );
		},
		nextUntil: function( elem, i, until ) {
			return dir( elem, "nextSibling", until );
		},
		prevUntil: function( elem, i, until ) {
			return dir( elem, "previousSibling", until );
		},
		siblings: function( elem ) {
			return siblings( ( elem.parentNode || {} ).firstChild, elem );
		},
		children: function( elem ) {
			return siblings( elem.firstChild );
		},
		contents: function( elem ) {
			return elem.contentDocument || jQuery.merge( [], elem.childNodes );
		}
	}, function( name, fn ) {
		jQuery.fn[ name ] = function( until, selector ) {
			var matched = jQuery.map( this, fn, until );

			if ( name.slice( -5 ) !== "Until" ) {
				selector = until;
			}

			if ( selector && typeof selector === "string" ) {
				matched = jQuery.filter( selector, matched );
			}

			if ( this.length > 1 ) {

				// Remove duplicates
				if ( !guaranteedUnique[ name ] ) {
					jQuery.uniqueSort( matched );
				}

				// Reverse order for parents* and prev-derivatives
				if ( rparentsprev.test( name ) ) {
					matched.reverse();
				}
			}

			return this.pushStack( matched );
		};
	} );
	var rnotwhite = ( /\S+/g );



	// Convert String-formatted options into Object-formatted ones
	function createOptions( options ) {
		var object = {};
		jQuery.each( options.match( rnotwhite ) || [], function( _, flag ) {
			object[ flag ] = true;
		} );
		return object;
	}

	/*
	 * Create a callback list using the following parameters:
	 *
	 *	options: an optional list of space-separated options that will change how
	 *			the callback list behaves or a more traditional option object
	 *
	 * By default a callback list will act like an event callback list and can be
	 * "fired" multiple times.
	 *
	 * Possible options:
	 *
	 *	once:			will ensure the callback list can only be fired once (like a Deferred)
	 *
	 *	memory:			will keep track of previous values and will call any callback added
	 *					after the list has been fired right away with the latest "memorized"
	 *					values (like a Deferred)
	 *
	 *	unique:			will ensure a callback can only be added once (no duplicate in the list)
	 *
	 *	stopOnFalse:	interrupt callings when a callback returns false
	 *
	 */
	jQuery.Callbacks = function( options ) {

		// Convert options from String-formatted to Object-formatted if needed
		// (we check in cache first)
		options = typeof options === "string" ?
			createOptions( options ) :
			jQuery.extend( {}, options );

		var // Flag to know if list is currently firing
			firing,

			// Last fire value for non-forgettable lists
			memory,

			// Flag to know if list was already fired
			fired,

			// Flag to prevent firing
			locked,

			// Actual callback list
			list = [],

			// Queue of execution data for repeatable lists
			queue = [],

			// Index of currently firing callback (modified by add/remove as needed)
			firingIndex = -1,

			// Fire callbacks
			fire = function() {

				// Enforce single-firing
				locked = options.once;

				// Execute callbacks for all pending executions,
				// respecting firingIndex overrides and runtime changes
				fired = firing = true;
				for ( ; queue.length; firingIndex = -1 ) {
					memory = queue.shift();
					while ( ++firingIndex < list.length ) {

						// Run callback and check for early termination
						if ( list[ firingIndex ].apply( memory[ 0 ], memory[ 1 ] ) === false &&
							options.stopOnFalse ) {

							// Jump to end and forget the data so .add doesn't re-fire
							firingIndex = list.length;
							memory = false;
						}
					}
				}

				// Forget the data if we're done with it
				if ( !options.memory ) {
					memory = false;
				}

				firing = false;

				// Clean up if we're done firing for good
				if ( locked ) {

					// Keep an empty list if we have data for future add calls
					if ( memory ) {
						list = [];

					// Otherwise, this object is spent
					} else {
						list = "";
					}
				}
			},

			// Actual Callbacks object
			self = {

				// Add a callback or a collection of callbacks to the list
				add: function() {
					if ( list ) {

						// If we have memory from a past run, we should fire after adding
						if ( memory && !firing ) {
							firingIndex = list.length - 1;
							queue.push( memory );
						}

						( function add( args ) {
							jQuery.each( args, function( _, arg ) {
								if ( jQuery.isFunction( arg ) ) {
									if ( !options.unique || !self.has( arg ) ) {
										list.push( arg );
									}
								} else if ( arg && arg.length && jQuery.type( arg ) !== "string" ) {

									// Inspect recursively
									add( arg );
								}
							} );
						} )( arguments );

						if ( memory && !firing ) {
							fire();
						}
					}
					return this;
				},

				// Remove a callback from the list
				remove: function() {
					jQuery.each( arguments, function( _, arg ) {
						var index;
						while ( ( index = jQuery.inArray( arg, list, index ) ) > -1 ) {
							list.splice( index, 1 );

							// Handle firing indexes
							if ( index <= firingIndex ) {
								firingIndex--;
							}
						}
					} );
					return this;
				},

				// Check if a given callback is in the list.
				// If no argument is given, return whether or not list has callbacks attached.
				has: function( fn ) {
					return fn ?
						jQuery.inArray( fn, list ) > -1 :
						list.length > 0;
				},

				// Remove all callbacks from the list
				empty: function() {
					if ( list ) {
						list = [];
					}
					return this;
				},

				// Disable .fire and .add
				// Abort any current/pending executions
				// Clear all callbacks and values
				disable: function() {
					locked = queue = [];
					list = memory = "";
					return this;
				},
				disabled: function() {
					return !list;
				},

				// Disable .fire
				// Also disable .add unless we have memory (since it would have no effect)
				// Abort any pending executions
				lock: function() {
					locked = queue = [];
					if ( !memory && !firing ) {
						list = memory = "";
					}
					return this;
				},
				locked: function() {
					return !!locked;
				},

				// Call all callbacks with the given context and arguments
				fireWith: function( context, args ) {
					if ( !locked ) {
						args = args || [];
						args = [ context, args.slice ? args.slice() : args ];
						queue.push( args );
						if ( !firing ) {
							fire();
						}
					}
					return this;
				},

				// Call all the callbacks with the given arguments
				fire: function() {
					self.fireWith( this, arguments );
					return this;
				},

				// To know if the callbacks have already been called at least once
				fired: function() {
					return !!fired;
				}
			};

		return self;
	};


	function Identity( v ) {
		return v;
	}
	function Thrower( ex ) {
		throw ex;
	}

	function adoptValue( value, resolve, reject ) {
		var method;

		try {

			// Check for promise aspect first to privilege synchronous behavior
			if ( value && jQuery.isFunction( ( method = value.promise ) ) ) {
				method.call( value ).done( resolve ).fail( reject );

			// Other thenables
			} else if ( value && jQuery.isFunction( ( method = value.then ) ) ) {
				method.call( value, resolve, reject );

			// Other non-thenables
			} else {

				// Support: Android 4.0 only
				// Strict mode functions invoked without .call/.apply get global-object context
				resolve.call( undefined, value );
			}

		// For Promises/A+, convert exceptions into rejections
		// Since jQuery.when doesn't unwrap thenables, we can skip the extra checks appearing in
		// Deferred#then to conditionally suppress rejection.
		} catch ( value ) {

			// Support: Android 4.0 only
			// Strict mode functions invoked without .call/.apply get global-object context
			reject.call( undefined, value );
		}
	}

	jQuery.extend( {

		Deferred: function( func ) {
			var tuples = [

					// action, add listener, callbacks,
					// ... .then handlers, argument index, [final state]
					[ "notify", "progress", jQuery.Callbacks( "memory" ),
						jQuery.Callbacks( "memory" ), 2 ],
					[ "resolve", "done", jQuery.Callbacks( "once memory" ),
						jQuery.Callbacks( "once memory" ), 0, "resolved" ],
					[ "reject", "fail", jQuery.Callbacks( "once memory" ),
						jQuery.Callbacks( "once memory" ), 1, "rejected" ]
				],
				state = "pending",
				promise = {
					state: function() {
						return state;
					},
					always: function() {
						deferred.done( arguments ).fail( arguments );
						return this;
					},
					"catch": function( fn ) {
						return promise.then( null, fn );
					},

					// Keep pipe for back-compat
					pipe: function( /* fnDone, fnFail, fnProgress */ ) {
						var fns = arguments;

						return jQuery.Deferred( function( newDefer ) {
							jQuery.each( tuples, function( i, tuple ) {

								// Map tuples (progress, done, fail) to arguments (done, fail, progress)
								var fn = jQuery.isFunction( fns[ tuple[ 4 ] ] ) && fns[ tuple[ 4 ] ];

								// deferred.progress(function() { bind to newDefer or newDefer.notify })
								// deferred.done(function() { bind to newDefer or newDefer.resolve })
								// deferred.fail(function() { bind to newDefer or newDefer.reject })
								deferred[ tuple[ 1 ] ]( function() {
									var returned = fn && fn.apply( this, arguments );
									if ( returned && jQuery.isFunction( returned.promise ) ) {
										returned.promise()
											.progress( newDefer.notify )
											.done( newDefer.resolve )
											.fail( newDefer.reject );
									} else {
										newDefer[ tuple[ 0 ] + "With" ](
											this,
											fn ? [ returned ] : arguments
										);
									}
								} );
							} );
							fns = null;
						} ).promise();
					},
					then: function( onFulfilled, onRejected, onProgress ) {
						var maxDepth = 0;
						function resolve( depth, deferred, handler, special ) {
							return function() {
								var that = this,
									args = arguments,
									mightThrow = function() {
										var returned, then;

										// Support: Promises/A+ section 2.3.3.3.3
										// https://promisesaplus.com/#point-59
										// Ignore double-resolution attempts
										if ( depth < maxDepth ) {
											return;
										}

										returned = handler.apply( that, args );

										// Support: Promises/A+ section 2.3.1
										// https://promisesaplus.com/#point-48
										if ( returned === deferred.promise() ) {
											throw new TypeError( "Thenable self-resolution" );
										}

										// Support: Promises/A+ sections 2.3.3.1, 3.5
										// https://promisesaplus.com/#point-54
										// https://promisesaplus.com/#point-75
										// Retrieve `then` only once
										then = returned &&

											// Support: Promises/A+ section 2.3.4
											// https://promisesaplus.com/#point-64
											// Only check objects and functions for thenability
											( typeof returned === "object" ||
												typeof returned === "function" ) &&
											returned.then;

										// Handle a returned thenable
										if ( jQuery.isFunction( then ) ) {

											// Special processors (notify) just wait for resolution
											if ( special ) {
												then.call(
													returned,
													resolve( maxDepth, deferred, Identity, special ),
													resolve( maxDepth, deferred, Thrower, special )
												);

											// Normal processors (resolve) also hook into progress
											} else {

												// ...and disregard older resolution values
												maxDepth++;

												then.call(
													returned,
													resolve( maxDepth, deferred, Identity, special ),
													resolve( maxDepth, deferred, Thrower, special ),
													resolve( maxDepth, deferred, Identity,
														deferred.notifyWith )
												);
											}

										// Handle all other returned values
										} else {

											// Only substitute handlers pass on context
											// and multiple values (non-spec behavior)
											if ( handler !== Identity ) {
												that = undefined;
												args = [ returned ];
											}

											// Process the value(s)
											// Default process is resolve
											( special || deferred.resolveWith )( that, args );
										}
									},

									// Only normal processors (resolve) catch and reject exceptions
									process = special ?
										mightThrow :
										function() {
											try {
												mightThrow();
											} catch ( e ) {

												if ( jQuery.Deferred.exceptionHook ) {
													jQuery.Deferred.exceptionHook( e,
														process.stackTrace );
												}

												// Support: Promises/A+ section 2.3.3.3.4.1
												// https://promisesaplus.com/#point-61
												// Ignore post-resolution exceptions
												if ( depth + 1 >= maxDepth ) {

													// Only substitute handlers pass on context
													// and multiple values (non-spec behavior)
													if ( handler !== Thrower ) {
														that = undefined;
														args = [ e ];
													}

													deferred.rejectWith( that, args );
												}
											}
										};

								// Support: Promises/A+ section 2.3.3.3.1
								// https://promisesaplus.com/#point-57
								// Re-resolve promises immediately to dodge false rejection from
								// subsequent errors
								if ( depth ) {
									process();
								} else {

									// Call an optional hook to record the stack, in case of exception
									// since it's otherwise lost when execution goes async
									if ( jQuery.Deferred.getStackHook ) {
										process.stackTrace = jQuery.Deferred.getStackHook();
									}
									window.setTimeout( process );
								}
							};
						}

						return jQuery.Deferred( function( newDefer ) {

							// progress_handlers.add( ... )
							tuples[ 0 ][ 3 ].add(
								resolve(
									0,
									newDefer,
									jQuery.isFunction( onProgress ) ?
										onProgress :
										Identity,
									newDefer.notifyWith
								)
							);

							// fulfilled_handlers.add( ... )
							tuples[ 1 ][ 3 ].add(
								resolve(
									0,
									newDefer,
									jQuery.isFunction( onFulfilled ) ?
										onFulfilled :
										Identity
								)
							);

							// rejected_handlers.add( ... )
							tuples[ 2 ][ 3 ].add(
								resolve(
									0,
									newDefer,
									jQuery.isFunction( onRejected ) ?
										onRejected :
										Thrower
								)
							);
						} ).promise();
					},

					// Get a promise for this deferred
					// If obj is provided, the promise aspect is added to the object
					promise: function( obj ) {
						return obj != null ? jQuery.extend( obj, promise ) : promise;
					}
				},
				deferred = {};

			// Add list-specific methods
			jQuery.each( tuples, function( i, tuple ) {
				var list = tuple[ 2 ],
					stateString = tuple[ 5 ];

				// promise.progress = list.add
				// promise.done = list.add
				// promise.fail = list.add
				promise[ tuple[ 1 ] ] = list.add;

				// Handle state
				if ( stateString ) {
					list.add(
						function() {

							// state = "resolved" (i.e., fulfilled)
							// state = "rejected"
							state = stateString;
						},

						// rejected_callbacks.disable
						// fulfilled_callbacks.disable
						tuples[ 3 - i ][ 2 ].disable,

						// progress_callbacks.lock
						tuples[ 0 ][ 2 ].lock
					);
				}

				// progress_handlers.fire
				// fulfilled_handlers.fire
				// rejected_handlers.fire
				list.add( tuple[ 3 ].fire );

				// deferred.notify = function() { deferred.notifyWith(...) }
				// deferred.resolve = function() { deferred.resolveWith(...) }
				// deferred.reject = function() { deferred.rejectWith(...) }
				deferred[ tuple[ 0 ] ] = function() {
					deferred[ tuple[ 0 ] + "With" ]( this === deferred ? undefined : this, arguments );
					return this;
				};

				// deferred.notifyWith = list.fireWith
				// deferred.resolveWith = list.fireWith
				// deferred.rejectWith = list.fireWith
				deferred[ tuple[ 0 ] + "With" ] = list.fireWith;
			} );

			// Make the deferred a promise
			promise.promise( deferred );

			// Call given func if any
			if ( func ) {
				func.call( deferred, deferred );
			}

			// All done!
			return deferred;
		},

		// Deferred helper
		when: function( singleValue ) {
			var

				// count of uncompleted subordinates
				remaining = arguments.length,

				// count of unprocessed arguments
				i = remaining,

				// subordinate fulfillment data
				resolveContexts = Array( i ),
				resolveValues = slice.call( arguments ),

				// the master Deferred
				master = jQuery.Deferred(),

				// subordinate callback factory
				updateFunc = function( i ) {
					return function( value ) {
						resolveContexts[ i ] = this;
						resolveValues[ i ] = arguments.length > 1 ? slice.call( arguments ) : value;
						if ( !( --remaining ) ) {
							master.resolveWith( resolveContexts, resolveValues );
						}
					};
				};

			// Single- and empty arguments are adopted like Promise.resolve
			if ( remaining <= 1 ) {
				adoptValue( singleValue, master.done( updateFunc( i ) ).resolve, master.reject );

				// Use .then() to unwrap secondary thenables (cf. gh-3000)
				if ( master.state() === "pending" ||
					jQuery.isFunction( resolveValues[ i ] && resolveValues[ i ].then ) ) {

					return master.then();
				}
			}

			// Multiple arguments are aggregated like Promise.all array elements
			while ( i-- ) {
				adoptValue( resolveValues[ i ], updateFunc( i ), master.reject );
			}

			return master.promise();
		}
	} );


	// These usually indicate a programmer mistake during development,
	// warn about them ASAP rather than swallowing them by default.
	var rerrorNames = /^(Eval|Internal|Range|Reference|Syntax|Type|URI)Error$/;

	jQuery.Deferred.exceptionHook = function( error, stack ) {

		// Support: IE 8 - 9 only
		// Console exists when dev tools are open, which can happen at any time
		if ( window.console && window.console.warn && error && rerrorNames.test( error.name ) ) {
			window.console.warn( "jQuery.Deferred exception: " + error.message, error.stack, stack );
		}
	};




	jQuery.readyException = function( error ) {
		window.setTimeout( function() {
			throw error;
		} );
	};




	// The deferred used on DOM ready
	var readyList = jQuery.Deferred();

	jQuery.fn.ready = function( fn ) {

		readyList
			.then( fn )

			// Wrap jQuery.readyException in a function so that the lookup
			// happens at the time of error handling instead of callback
			// registration.
			.catch( function( error ) {
				jQuery.readyException( error );
			} );

		return this;
	};

	jQuery.extend( {

		// Is the DOM ready to be used? Set to true once it occurs.
		isReady: false,

		// A counter to track how many items to wait for before
		// the ready event fires. See #6781
		readyWait: 1,

		// Hold (or release) the ready event
		holdReady: function( hold ) {
			if ( hold ) {
				jQuery.readyWait++;
			} else {
				jQuery.ready( true );
			}
		},

		// Handle when the DOM is ready
		ready: function( wait ) {

			// Abort if there are pending holds or we're already ready
			if ( wait === true ? --jQuery.readyWait : jQuery.isReady ) {
				return;
			}

			// Remember that the DOM is ready
			jQuery.isReady = true;

			// If a normal DOM Ready event fired, decrement, and wait if need be
			if ( wait !== true && --jQuery.readyWait > 0 ) {
				return;
			}

			// If there are functions bound, to execute
			readyList.resolveWith( document, [ jQuery ] );
		}
	} );

	jQuery.ready.then = readyList.then;

	// The ready event handler and self cleanup method
	function completed() {
		document.removeEventListener( "DOMContentLoaded", completed );
		window.removeEventListener( "load", completed );
		jQuery.ready();
	}

	// Catch cases where $(document).ready() is called
	// after the browser event has already occurred.
	// Support: IE <=9 - 10 only
	// Older IE sometimes signals "interactive" too soon
	if ( document.readyState === "complete" ||
		( document.readyState !== "loading" && !document.documentElement.doScroll ) ) {

		// Handle it asynchronously to allow scripts the opportunity to delay ready
		window.setTimeout( jQuery.ready );

	} else {

		// Use the handy event callback
		document.addEventListener( "DOMContentLoaded", completed );

		// A fallback to window.onload, that will always work
		window.addEventListener( "load", completed );
	}




	// Multifunctional method to get and set values of a collection
	// The value/s can optionally be executed if it's a function
	var access = function( elems, fn, key, value, chainable, emptyGet, raw ) {
		var i = 0,
			len = elems.length,
			bulk = key == null;

		// Sets many values
		if ( jQuery.type( key ) === "object" ) {
			chainable = true;
			for ( i in key ) {
				access( elems, fn, i, key[ i ], true, emptyGet, raw );
			}

		// Sets one value
		} else if ( value !== undefined ) {
			chainable = true;

			if ( !jQuery.isFunction( value ) ) {
				raw = true;
			}

			if ( bulk ) {

				// Bulk operations run against the entire set
				if ( raw ) {
					fn.call( elems, value );
					fn = null;

				// ...except when executing function values
				} else {
					bulk = fn;
					fn = function( elem, key, value ) {
						return bulk.call( jQuery( elem ), value );
					};
				}
			}

			if ( fn ) {
				for ( ; i < len; i++ ) {
					fn(
						elems[ i ], key, raw ?
						value :
						value.call( elems[ i ], i, fn( elems[ i ], key ) )
					);
				}
			}
		}

		return chainable ?
			elems :

			// Gets
			bulk ?
				fn.call( elems ) :
				len ? fn( elems[ 0 ], key ) : emptyGet;
	};
	var acceptData = function( owner ) {

		// Accepts only:
		//  - Node
		//    - Node.ELEMENT_NODE
		//    - Node.DOCUMENT_NODE
		//  - Object
		//    - Any
		return owner.nodeType === 1 || owner.nodeType === 9 || !( +owner.nodeType );
	};




	function Data() {
		this.expando = jQuery.expando + Data.uid++;
	}

	Data.uid = 1;

	Data.prototype = {

		cache: function( owner ) {

			// Check if the owner object already has a cache
			var value = owner[ this.expando ];

			// If not, create one
			if ( !value ) {
				value = {};

				// We can accept data for non-element nodes in modern browsers,
				// but we should not, see #8335.
				// Always return an empty object.
				if ( acceptData( owner ) ) {

					// If it is a node unlikely to be stringify-ed or looped over
					// use plain assignment
					if ( owner.nodeType ) {
						owner[ this.expando ] = value;

					// Otherwise secure it in a non-enumerable property
					// configurable must be true to allow the property to be
					// deleted when data is removed
					} else {
						Object.defineProperty( owner, this.expando, {
							value: value,
							configurable: true
						} );
					}
				}
			}

			return value;
		},
		set: function( owner, data, value ) {
			var prop,
				cache = this.cache( owner );

			// Handle: [ owner, key, value ] args
			// Always use camelCase key (gh-2257)
			if ( typeof data === "string" ) {
				cache[ jQuery.camelCase( data ) ] = value;

			// Handle: [ owner, { properties } ] args
			} else {

				// Copy the properties one-by-one to the cache object
				for ( prop in data ) {
					cache[ jQuery.camelCase( prop ) ] = data[ prop ];
				}
			}
			return cache;
		},
		get: function( owner, key ) {
			return key === undefined ?
				this.cache( owner ) :

				// Always use camelCase key (gh-2257)
				owner[ this.expando ] && owner[ this.expando ][ jQuery.camelCase( key ) ];
		},
		access: function( owner, key, value ) {

			// In cases where either:
			//
			//   1. No key was specified
			//   2. A string key was specified, but no value provided
			//
			// Take the "read" path and allow the get method to determine
			// which value to return, respectively either:
			//
			//   1. The entire cache object
			//   2. The data stored at the key
			//
			if ( key === undefined ||
					( ( key && typeof key === "string" ) && value === undefined ) ) {

				return this.get( owner, key );
			}

			// When the key is not a string, or both a key and value
			// are specified, set or extend (existing objects) with either:
			//
			//   1. An object of properties
			//   2. A key and value
			//
			this.set( owner, key, value );

			// Since the "set" path can have two possible entry points
			// return the expected data based on which path was taken[*]
			return value !== undefined ? value : key;
		},
		remove: function( owner, key ) {
			var i,
				cache = owner[ this.expando ];

			if ( cache === undefined ) {
				return;
			}

			if ( key !== undefined ) {

				// Support array or space separated string of keys
				if ( jQuery.isArray( key ) ) {

					// If key is an array of keys...
					// We always set camelCase keys, so remove that.
					key = key.map( jQuery.camelCase );
				} else {
					key = jQuery.camelCase( key );

					// If a key with the spaces exists, use it.
					// Otherwise, create an array by matching non-whitespace
					key = key in cache ?
						[ key ] :
						( key.match( rnotwhite ) || [] );
				}

				i = key.length;

				while ( i-- ) {
					delete cache[ key[ i ] ];
				}
			}

			// Remove the expando if there's no more data
			if ( key === undefined || jQuery.isEmptyObject( cache ) ) {

				// Support: Chrome <=35 - 45
				// Webkit & Blink performance suffers when deleting properties
				// from DOM nodes, so set to undefined instead
				// https://bugs.chromium.org/p/chromium/issues/detail?id=378607 (bug restricted)
				if ( owner.nodeType ) {
					owner[ this.expando ] = undefined;
				} else {
					delete owner[ this.expando ];
				}
			}
		},
		hasData: function( owner ) {
			var cache = owner[ this.expando ];
			return cache !== undefined && !jQuery.isEmptyObject( cache );
		}
	};
	var dataPriv = new Data();

	var dataUser = new Data();



	//	Implementation Summary
	//
	//	1. Enforce API surface and semantic compatibility with 1.9.x branch
	//	2. Improve the module's maintainability by reducing the storage
	//		paths to a single mechanism.
	//	3. Use the same single mechanism to support "private" and "user" data.
	//	4. _Never_ expose "private" data to user code (TODO: Drop _data, _removeData)
	//	5. Avoid exposing implementation details on user objects (eg. expando properties)
	//	6. Provide a clear path for implementation upgrade to WeakMap in 2014

	var rbrace = /^(?:\{[\w\W]*\}|\[[\w\W]*\])$/,
		rmultiDash = /[A-Z]/g;

	function dataAttr( elem, key, data ) {
		var name;

		// If nothing was found internally, try to fetch any
		// data from the HTML5 data-* attribute
		if ( data === undefined && elem.nodeType === 1 ) {
			name = "data-" + key.replace( rmultiDash, "-$&" ).toLowerCase();
			data = elem.getAttribute( name );

			if ( typeof data === "string" ) {
				try {
					data = data === "true" ? true :
						data === "false" ? false :
						data === "null" ? null :

						// Only convert to a number if it doesn't change the string
						+data + "" === data ? +data :
						rbrace.test( data ) ? JSON.parse( data ) :
						data;
				} catch ( e ) {}

				// Make sure we set the data so it isn't changed later
				dataUser.set( elem, key, data );
			} else {
				data = undefined;
			}
		}
		return data;
	}

	jQuery.extend( {
		hasData: function( elem ) {
			return dataUser.hasData( elem ) || dataPriv.hasData( elem );
		},

		data: function( elem, name, data ) {
			return dataUser.access( elem, name, data );
		},

		removeData: function( elem, name ) {
			dataUser.remove( elem, name );
		},

		// TODO: Now that all calls to _data and _removeData have been replaced
		// with direct calls to dataPriv methods, these can be deprecated.
		_data: function( elem, name, data ) {
			return dataPriv.access( elem, name, data );
		},

		_removeData: function( elem, name ) {
			dataPriv.remove( elem, name );
		}
	} );

	jQuery.fn.extend( {
		data: function( key, value ) {
			var i, name, data,
				elem = this[ 0 ],
				attrs = elem && elem.attributes;

			// Gets all values
			if ( key === undefined ) {
				if ( this.length ) {
					data = dataUser.get( elem );

					if ( elem.nodeType === 1 && !dataPriv.get( elem, "hasDataAttrs" ) ) {
						i = attrs.length;
						while ( i-- ) {

							// Support: IE 11 only
							// The attrs elements can be null (#14894)
							if ( attrs[ i ] ) {
								name = attrs[ i ].name;
								if ( name.indexOf( "data-" ) === 0 ) {
									name = jQuery.camelCase( name.slice( 5 ) );
									dataAttr( elem, name, data[ name ] );
								}
							}
						}
						dataPriv.set( elem, "hasDataAttrs", true );
					}
				}

				return data;
			}

			// Sets multiple values
			if ( typeof key === "object" ) {
				return this.each( function() {
					dataUser.set( this, key );
				} );
			}

			return access( this, function( value ) {
				var data;

				// The calling jQuery object (element matches) is not empty
				// (and therefore has an element appears at this[ 0 ]) and the
				// `value` parameter was not undefined. An empty jQuery object
				// will result in `undefined` for elem = this[ 0 ] which will
				// throw an exception if an attempt to read a data cache is made.
				if ( elem && value === undefined ) {

					// Attempt to get data from the cache
					// The key will always be camelCased in Data
					data = dataUser.get( elem, key );
					if ( data !== undefined ) {
						return data;
					}

					// Attempt to "discover" the data in
					// HTML5 custom data-* attrs
					data = dataAttr( elem, key );
					if ( data !== undefined ) {
						return data;
					}

					// We tried really hard, but the data doesn't exist.
					return;
				}

				// Set the data...
				this.each( function() {

					// We always store the camelCased key
					dataUser.set( this, key, value );
				} );
			}, null, value, arguments.length > 1, null, true );
		},

		removeData: function( key ) {
			return this.each( function() {
				dataUser.remove( this, key );
			} );
		}
	} );


	jQuery.extend( {
		queue: function( elem, type, data ) {
			var queue;

			if ( elem ) {
				type = ( type || "fx" ) + "queue";
				queue = dataPriv.get( elem, type );

				// Speed up dequeue by getting out quickly if this is just a lookup
				if ( data ) {
					if ( !queue || jQuery.isArray( data ) ) {
						queue = dataPriv.access( elem, type, jQuery.makeArray( data ) );
					} else {
						queue.push( data );
					}
				}
				return queue || [];
			}
		},

		dequeue: function( elem, type ) {
			type = type || "fx";

			var queue = jQuery.queue( elem, type ),
				startLength = queue.length,
				fn = queue.shift(),
				hooks = jQuery._queueHooks( elem, type ),
				next = function() {
					jQuery.dequeue( elem, type );
				};

			// If the fx queue is dequeued, always remove the progress sentinel
			if ( fn === "inprogress" ) {
				fn = queue.shift();
				startLength--;
			}

			if ( fn ) {

				// Add a progress sentinel to prevent the fx queue from being
				// automatically dequeued
				if ( type === "fx" ) {
					queue.unshift( "inprogress" );
				}

				// Clear up the last queue stop function
				delete hooks.stop;
				fn.call( elem, next, hooks );
			}

			if ( !startLength && hooks ) {
				hooks.empty.fire();
			}
		},

		// Not public - generate a queueHooks object, or return the current one
		_queueHooks: function( elem, type ) {
			var key = type + "queueHooks";
			return dataPriv.get( elem, key ) || dataPriv.access( elem, key, {
				empty: jQuery.Callbacks( "once memory" ).add( function() {
					dataPriv.remove( elem, [ type + "queue", key ] );
				} )
			} );
		}
	} );

	jQuery.fn.extend( {
		queue: function( type, data ) {
			var setter = 2;

			if ( typeof type !== "string" ) {
				data = type;
				type = "fx";
				setter--;
			}

			if ( arguments.length < setter ) {
				return jQuery.queue( this[ 0 ], type );
			}

			return data === undefined ?
				this :
				this.each( function() {
					var queue = jQuery.queue( this, type, data );

					// Ensure a hooks for this queue
					jQuery._queueHooks( this, type );

					if ( type === "fx" && queue[ 0 ] !== "inprogress" ) {
						jQuery.dequeue( this, type );
					}
				} );
		},
		dequeue: function( type ) {
			return this.each( function() {
				jQuery.dequeue( this, type );
			} );
		},
		clearQueue: function( type ) {
			return this.queue( type || "fx", [] );
		},

		// Get a promise resolved when queues of a certain type
		// are emptied (fx is the type by default)
		promise: function( type, obj ) {
			var tmp,
				count = 1,
				defer = jQuery.Deferred(),
				elements = this,
				i = this.length,
				resolve = function() {
					if ( !( --count ) ) {
						defer.resolveWith( elements, [ elements ] );
					}
				};

			if ( typeof type !== "string" ) {
				obj = type;
				type = undefined;
			}
			type = type || "fx";

			while ( i-- ) {
				tmp = dataPriv.get( elements[ i ], type + "queueHooks" );
				if ( tmp && tmp.empty ) {
					count++;
					tmp.empty.add( resolve );
				}
			}
			resolve();
			return defer.promise( obj );
		}
	} );
	var pnum = ( /[+-]?(?:\d*\.|)\d+(?:[eE][+-]?\d+|)/ ).source;

	var rcssNum = new RegExp( "^(?:([+-])=|)(" + pnum + ")([a-z%]*)$", "i" );


	var cssExpand = [ "Top", "Right", "Bottom", "Left" ];

	var isHiddenWithinTree = function( elem, el ) {

			// isHiddenWithinTree might be called from jQuery#filter function;
			// in that case, element will be second argument
			elem = el || elem;

			// Inline style trumps all
			return elem.style.display === "none" ||
				elem.style.display === "" &&

				// Otherwise, check computed style
				// Support: Firefox <=43 - 45
				// Disconnected elements can have computed display: none, so first confirm that elem is
				// in the document.
				jQuery.contains( elem.ownerDocument, elem ) &&

				jQuery.css( elem, "display" ) === "none";
		};

	var swap = function( elem, options, callback, args ) {
		var ret, name,
			old = {};

		// Remember the old values, and insert the new ones
		for ( name in options ) {
			old[ name ] = elem.style[ name ];
			elem.style[ name ] = options[ name ];
		}

		ret = callback.apply( elem, args || [] );

		// Revert the old values
		for ( name in options ) {
			elem.style[ name ] = old[ name ];
		}

		return ret;
	};




	function adjustCSS( elem, prop, valueParts, tween ) {
		var adjusted,
			scale = 1,
			maxIterations = 20,
			currentValue = tween ?
				function() {
					return tween.cur();
				} :
				function() {
					return jQuery.css( elem, prop, "" );
				},
			initial = currentValue(),
			unit = valueParts && valueParts[ 3 ] || ( jQuery.cssNumber[ prop ] ? "" : "px" ),

			// Starting value computation is required for potential unit mismatches
			initialInUnit = ( jQuery.cssNumber[ prop ] || unit !== "px" && +initial ) &&
				rcssNum.exec( jQuery.css( elem, prop ) );

		if ( initialInUnit && initialInUnit[ 3 ] !== unit ) {

			// Trust units reported by jQuery.css
			unit = unit || initialInUnit[ 3 ];

			// Make sure we update the tween properties later on
			valueParts = valueParts || [];

			// Iteratively approximate from a nonzero starting point
			initialInUnit = +initial || 1;

			do {

				// If previous iteration zeroed out, double until we get *something*.
				// Use string for doubling so we don't accidentally see scale as unchanged below
				scale = scale || ".5";

				// Adjust and apply
				initialInUnit = initialInUnit / scale;
				jQuery.style( elem, prop, initialInUnit + unit );

			// Update scale, tolerating zero or NaN from tween.cur()
			// Break the loop if scale is unchanged or perfect, or if we've just had enough.
			} while (
				scale !== ( scale = currentValue() / initial ) && scale !== 1 && --maxIterations
			);
		}

		if ( valueParts ) {
			initialInUnit = +initialInUnit || +initial || 0;

			// Apply relative offset (+=/-=) if specified
			adjusted = valueParts[ 1 ] ?
				initialInUnit + ( valueParts[ 1 ] + 1 ) * valueParts[ 2 ] :
				+valueParts[ 2 ];
			if ( tween ) {
				tween.unit = unit;
				tween.start = initialInUnit;
				tween.end = adjusted;
			}
		}
		return adjusted;
	}


	var defaultDisplayMap = {};

	function getDefaultDisplay( elem ) {
		var temp,
			doc = elem.ownerDocument,
			nodeName = elem.nodeName,
			display = defaultDisplayMap[ nodeName ];

		if ( display ) {
			return display;
		}

		temp = doc.body.appendChild( doc.createElement( nodeName ) ),
		display = jQuery.css( temp, "display" );

		temp.parentNode.removeChild( temp );

		if ( display === "none" ) {
			display = "block";
		}
		defaultDisplayMap[ nodeName ] = display;

		return display;
	}

	function showHide( elements, show ) {
		var display, elem,
			values = [],
			index = 0,
			length = elements.length;

		// Determine new display value for elements that need to change
		for ( ; index < length; index++ ) {
			elem = elements[ index ];
			if ( !elem.style ) {
				continue;
			}

			display = elem.style.display;
			if ( show ) {

				// Since we force visibility upon cascade-hidden elements, an immediate (and slow)
				// check is required in this first loop unless we have a nonempty display value (either
				// inline or about-to-be-restored)
				if ( display === "none" ) {
					values[ index ] = dataPriv.get( elem, "display" ) || null;
					if ( !values[ index ] ) {
						elem.style.display = "";
					}
				}
				if ( elem.style.display === "" && isHiddenWithinTree( elem ) ) {
					values[ index ] = getDefaultDisplay( elem );
				}
			} else {
				if ( display !== "none" ) {
					values[ index ] = "none";

					// Remember what we're overwriting
					dataPriv.set( elem, "display", display );
				}
			}
		}

		// Set the display of the elements in a second loop to avoid constant reflow
		for ( index = 0; index < length; index++ ) {
			if ( values[ index ] != null ) {
				elements[ index ].style.display = values[ index ];
			}
		}

		return elements;
	}

	jQuery.fn.extend( {
		show: function() {
			return showHide( this, true );
		},
		hide: function() {
			return showHide( this );
		},
		toggle: function( state ) {
			if ( typeof state === "boolean" ) {
				return state ? this.show() : this.hide();
			}

			return this.each( function() {
				if ( isHiddenWithinTree( this ) ) {
					jQuery( this ).show();
				} else {
					jQuery( this ).hide();
				}
			} );
		}
	} );
	var rcheckableType = ( /^(?:checkbox|radio)$/i );

	var rtagName = ( /<([a-z][^\/\0>\x20\t\r\n\f]+)/i );

	var rscriptType = ( /^$|\/(?:java|ecma)script/i );



	// We have to close these tags to support XHTML (#13200)
	var wrapMap = {

		// Support: IE <=9 only
		option: [ 1, "<select multiple='multiple'>", "</select>" ],

		// XHTML parsers do not magically insert elements in the
		// same way that tag soup parsers do. So we cannot shorten
		// this by omitting <tbody> or other required elements.
		thead: [ 1, "<table>", "</table>" ],
		col: [ 2, "<table><colgroup>", "</colgroup></table>" ],
		tr: [ 2, "<table><tbody>", "</tbody></table>" ],
		td: [ 3, "<table><tbody><tr>", "</tr></tbody></table>" ],

		_default: [ 0, "", "" ]
	};

	// Support: IE <=9 only
	wrapMap.optgroup = wrapMap.option;

	wrapMap.tbody = wrapMap.tfoot = wrapMap.colgroup = wrapMap.caption = wrapMap.thead;
	wrapMap.th = wrapMap.td;


	function getAll( context, tag ) {

		// Support: IE <=9 - 11 only
		// Use typeof to avoid zero-argument method invocation on host objects (#15151)
		var ret = typeof context.getElementsByTagName !== "undefined" ?
				context.getElementsByTagName( tag || "*" ) :
				typeof context.querySelectorAll !== "undefined" ?
					context.querySelectorAll( tag || "*" ) :
				[];

		return tag === undefined || tag && jQuery.nodeName( context, tag ) ?
			jQuery.merge( [ context ], ret ) :
			ret;
	}


	// Mark scripts as having already been evaluated
	function setGlobalEval( elems, refElements ) {
		var i = 0,
			l = elems.length;

		for ( ; i < l; i++ ) {
			dataPriv.set(
				elems[ i ],
				"globalEval",
				!refElements || dataPriv.get( refElements[ i ], "globalEval" )
			);
		}
	}


	var rhtml = /<|&#?\w+;/;

	function buildFragment( elems, context, scripts, selection, ignored ) {
		var elem, tmp, tag, wrap, contains, j,
			fragment = context.createDocumentFragment(),
			nodes = [],
			i = 0,
			l = elems.length;

		for ( ; i < l; i++ ) {
			elem = elems[ i ];

			if ( elem || elem === 0 ) {

				// Add nodes directly
				if ( jQuery.type( elem ) === "object" ) {

					// Support: Android <=4.0 only, PhantomJS 1 only
					// push.apply(_, arraylike) throws on ancient WebKit
					jQuery.merge( nodes, elem.nodeType ? [ elem ] : elem );

				// Convert non-html into a text node
				} else if ( !rhtml.test( elem ) ) {
					nodes.push( context.createTextNode( elem ) );

				// Convert html into DOM nodes
				} else {
					tmp = tmp || fragment.appendChild( context.createElement( "div" ) );

					// Deserialize a standard representation
					tag = ( rtagName.exec( elem ) || [ "", "" ] )[ 1 ].toLowerCase();
					wrap = wrapMap[ tag ] || wrapMap._default;
					tmp.innerHTML = wrap[ 1 ] + jQuery.htmlPrefilter( elem ) + wrap[ 2 ];

					// Descend through wrappers to the right content
					j = wrap[ 0 ];
					while ( j-- ) {
						tmp = tmp.lastChild;
					}

					// Support: Android <=4.0 only, PhantomJS 1 only
					// push.apply(_, arraylike) throws on ancient WebKit
					jQuery.merge( nodes, tmp.childNodes );

					// Remember the top-level container
					tmp = fragment.firstChild;

					// Ensure the created nodes are orphaned (#12392)
					tmp.textContent = "";
				}
			}
		}

		// Remove wrapper from fragment
		fragment.textContent = "";

		i = 0;
		while ( ( elem = nodes[ i++ ] ) ) {

			// Skip elements already in the context collection (trac-4087)
			if ( selection && jQuery.inArray( elem, selection ) > -1 ) {
				if ( ignored ) {
					ignored.push( elem );
				}
				continue;
			}

			contains = jQuery.contains( elem.ownerDocument, elem );

			// Append to fragment
			tmp = getAll( fragment.appendChild( elem ), "script" );

			// Preserve script evaluation history
			if ( contains ) {
				setGlobalEval( tmp );
			}

			// Capture executables
			if ( scripts ) {
				j = 0;
				while ( ( elem = tmp[ j++ ] ) ) {
					if ( rscriptType.test( elem.type || "" ) ) {
						scripts.push( elem );
					}
				}
			}
		}

		return fragment;
	}


	( function() {
		var fragment = document.createDocumentFragment(),
			div = fragment.appendChild( document.createElement( "div" ) ),
			input = document.createElement( "input" );

		// Support: Android 4.0 - 4.3 only
		// Check state lost if the name is set (#11217)
		// Support: Windows Web Apps (WWA)
		// `name` and `type` must use .setAttribute for WWA (#14901)
		input.setAttribute( "type", "radio" );
		input.setAttribute( "checked", "checked" );
		input.setAttribute( "name", "t" );

		div.appendChild( input );

		// Support: Android <=4.1 only
		// Older WebKit doesn't clone checked state correctly in fragments
		support.checkClone = div.cloneNode( true ).cloneNode( true ).lastChild.checked;

		// Support: IE <=11 only
		// Make sure textarea (and checkbox) defaultValue is properly cloned
		div.innerHTML = "<textarea>x</textarea>";
		support.noCloneChecked = !!div.cloneNode( true ).lastChild.defaultValue;
	} )();
	var documentElement = document.documentElement;



	var
		rkeyEvent = /^key/,
		rmouseEvent = /^(?:mouse|pointer|contextmenu|drag|drop)|click/,
		rtypenamespace = /^([^.]*)(?:\.(.+)|)/;

	function returnTrue() {
		return true;
	}

	function returnFalse() {
		return false;
	}

	// Support: IE <=9 only
	// See #13393 for more info
	function safeActiveElement() {
		try {
			return document.activeElement;
		} catch ( err ) { }
	}

	function on( elem, types, selector, data, fn, one ) {
		var origFn, type;

		// Types can be a map of types/handlers
		if ( typeof types === "object" ) {

			// ( types-Object, selector, data )
			if ( typeof selector !== "string" ) {

				// ( types-Object, data )
				data = data || selector;
				selector = undefined;
			}
			for ( type in types ) {
				on( elem, type, selector, data, types[ type ], one );
			}
			return elem;
		}

		if ( data == null && fn == null ) {

			// ( types, fn )
			fn = selector;
			data = selector = undefined;
		} else if ( fn == null ) {
			if ( typeof selector === "string" ) {

				// ( types, selector, fn )
				fn = data;
				data = undefined;
			} else {

				// ( types, data, fn )
				fn = data;
				data = selector;
				selector = undefined;
			}
		}
		if ( fn === false ) {
			fn = returnFalse;
		} else if ( !fn ) {
			return elem;
		}

		if ( one === 1 ) {
			origFn = fn;
			fn = function( event ) {

				// Can use an empty set, since event contains the info
				jQuery().off( event );
				return origFn.apply( this, arguments );
			};

			// Use same guid so caller can remove using origFn
			fn.guid = origFn.guid || ( origFn.guid = jQuery.guid++ );
		}
		return elem.each( function() {
			jQuery.event.add( this, types, fn, data, selector );
		} );
	}

	/*
	 * Helper functions for managing events -- not part of the public interface.
	 * Props to Dean Edwards' addEvent library for many of the ideas.
	 */
	jQuery.event = {

		global: {},

		add: function( elem, types, handler, data, selector ) {

			var handleObjIn, eventHandle, tmp,
				events, t, handleObj,
				special, handlers, type, namespaces, origType,
				elemData = dataPriv.get( elem );

			// Don't attach events to noData or text/comment nodes (but allow plain objects)
			if ( !elemData ) {
				return;
			}

			// Caller can pass in an object of custom data in lieu of the handler
			if ( handler.handler ) {
				handleObjIn = handler;
				handler = handleObjIn.handler;
				selector = handleObjIn.selector;
			}

			// Ensure that invalid selectors throw exceptions at attach time
			// Evaluate against documentElement in case elem is a non-element node (e.g., document)
			if ( selector ) {
				jQuery.find.matchesSelector( documentElement, selector );
			}

			// Make sure that the handler has a unique ID, used to find/remove it later
			if ( !handler.guid ) {
				handler.guid = jQuery.guid++;
			}

			// Init the element's event structure and main handler, if this is the first
			if ( !( events = elemData.events ) ) {
				events = elemData.events = {};
			}
			if ( !( eventHandle = elemData.handle ) ) {
				eventHandle = elemData.handle = function( e ) {

					// Discard the second event of a jQuery.event.trigger() and
					// when an event is called after a page has unloaded
					return typeof jQuery !== "undefined" && jQuery.event.triggered !== e.type ?
						jQuery.event.dispatch.apply( elem, arguments ) : undefined;
				};
			}

			// Handle multiple events separated by a space
			types = ( types || "" ).match( rnotwhite ) || [ "" ];
			t = types.length;
			while ( t-- ) {
				tmp = rtypenamespace.exec( types[ t ] ) || [];
				type = origType = tmp[ 1 ];
				namespaces = ( tmp[ 2 ] || "" ).split( "." ).sort();

				// There *must* be a type, no attaching namespace-only handlers
				if ( !type ) {
					continue;
				}

				// If event changes its type, use the special event handlers for the changed type
				special = jQuery.event.special[ type ] || {};

				// If selector defined, determine special event api type, otherwise given type
				type = ( selector ? special.delegateType : special.bindType ) || type;

				// Update special based on newly reset type
				special = jQuery.event.special[ type ] || {};

				// handleObj is passed to all event handlers
				handleObj = jQuery.extend( {
					type: type,
					origType: origType,
					data: data,
					handler: handler,
					guid: handler.guid,
					selector: selector,
					needsContext: selector && jQuery.expr.match.needsContext.test( selector ),
					namespace: namespaces.join( "." )
				}, handleObjIn );

				// Init the event handler queue if we're the first
				if ( !( handlers = events[ type ] ) ) {
					handlers = events[ type ] = [];
					handlers.delegateCount = 0;

					// Only use addEventListener if the special events handler returns false
					if ( !special.setup ||
						special.setup.call( elem, data, namespaces, eventHandle ) === false ) {

						if ( elem.addEventListener ) {
							elem.addEventListener( type, eventHandle );
						}
					}
				}

				if ( special.add ) {
					special.add.call( elem, handleObj );

					if ( !handleObj.handler.guid ) {
						handleObj.handler.guid = handler.guid;
					}
				}

				// Add to the element's handler list, delegates in front
				if ( selector ) {
					handlers.splice( handlers.delegateCount++, 0, handleObj );
				} else {
					handlers.push( handleObj );
				}

				// Keep track of which events have ever been used, for event optimization
				jQuery.event.global[ type ] = true;
			}

		},

		// Detach an event or set of events from an element
		remove: function( elem, types, handler, selector, mappedTypes ) {

			var j, origCount, tmp,
				events, t, handleObj,
				special, handlers, type, namespaces, origType,
				elemData = dataPriv.hasData( elem ) && dataPriv.get( elem );

			if ( !elemData || !( events = elemData.events ) ) {
				return;
			}

			// Once for each type.namespace in types; type may be omitted
			types = ( types || "" ).match( rnotwhite ) || [ "" ];
			t = types.length;
			while ( t-- ) {
				tmp = rtypenamespace.exec( types[ t ] ) || [];
				type = origType = tmp[ 1 ];
				namespaces = ( tmp[ 2 ] || "" ).split( "." ).sort();

				// Unbind all events (on this namespace, if provided) for the element
				if ( !type ) {
					for ( type in events ) {
						jQuery.event.remove( elem, type + types[ t ], handler, selector, true );
					}
					continue;
				}

				special = jQuery.event.special[ type ] || {};
				type = ( selector ? special.delegateType : special.bindType ) || type;
				handlers = events[ type ] || [];
				tmp = tmp[ 2 ] &&
					new RegExp( "(^|\\.)" + namespaces.join( "\\.(?:.*\\.|)" ) + "(\\.|$)" );

				// Remove matching events
				origCount = j = handlers.length;
				while ( j-- ) {
					handleObj = handlers[ j ];

					if ( ( mappedTypes || origType === handleObj.origType ) &&
						( !handler || handler.guid === handleObj.guid ) &&
						( !tmp || tmp.test( handleObj.namespace ) ) &&
						( !selector || selector === handleObj.selector ||
							selector === "**" && handleObj.selector ) ) {
						handlers.splice( j, 1 );

						if ( handleObj.selector ) {
							handlers.delegateCount--;
						}
						if ( special.remove ) {
							special.remove.call( elem, handleObj );
						}
					}
				}

				// Remove generic event handler if we removed something and no more handlers exist
				// (avoids potential for endless recursion during removal of special event handlers)
				if ( origCount && !handlers.length ) {
					if ( !special.teardown ||
						special.teardown.call( elem, namespaces, elemData.handle ) === false ) {

						jQuery.removeEvent( elem, type, elemData.handle );
					}

					delete events[ type ];
				}
			}

			// Remove data and the expando if it's no longer used
			if ( jQuery.isEmptyObject( events ) ) {
				dataPriv.remove( elem, "handle events" );
			}
		},

		dispatch: function( nativeEvent ) {

			// Make a writable jQuery.Event from the native event object
			var event = jQuery.event.fix( nativeEvent );

			var i, j, ret, matched, handleObj, handlerQueue,
				args = new Array( arguments.length ),
				handlers = ( dataPriv.get( this, "events" ) || {} )[ event.type ] || [],
				special = jQuery.event.special[ event.type ] || {};

			// Use the fix-ed jQuery.Event rather than the (read-only) native event
			args[ 0 ] = event;

			for ( i = 1; i < arguments.length; i++ ) {
				args[ i ] = arguments[ i ];
			}

			event.delegateTarget = this;

			// Call the preDispatch hook for the mapped type, and let it bail if desired
			if ( special.preDispatch && special.preDispatch.call( this, event ) === false ) {
				return;
			}

			// Determine handlers
			handlerQueue = jQuery.event.handlers.call( this, event, handlers );

			// Run delegates first; they may want to stop propagation beneath us
			i = 0;
			while ( ( matched = handlerQueue[ i++ ] ) && !event.isPropagationStopped() ) {
				event.currentTarget = matched.elem;

				j = 0;
				while ( ( handleObj = matched.handlers[ j++ ] ) &&
					!event.isImmediatePropagationStopped() ) {

					// Triggered event must either 1) have no namespace, or 2) have namespace(s)
					// a subset or equal to those in the bound event (both can have no namespace).
					if ( !event.rnamespace || event.rnamespace.test( handleObj.namespace ) ) {

						event.handleObj = handleObj;
						event.data = handleObj.data;

						ret = ( ( jQuery.event.special[ handleObj.origType ] || {} ).handle ||
							handleObj.handler ).apply( matched.elem, args );

						if ( ret !== undefined ) {
							if ( ( event.result = ret ) === false ) {
								event.preventDefault();
								event.stopPropagation();
							}
						}
					}
				}
			}

			// Call the postDispatch hook for the mapped type
			if ( special.postDispatch ) {
				special.postDispatch.call( this, event );
			}

			return event.result;
		},

		handlers: function( event, handlers ) {
			var i, matches, sel, handleObj,
				handlerQueue = [],
				delegateCount = handlers.delegateCount,
				cur = event.target;

			// Support: IE <=9
			// Find delegate handlers
			// Black-hole SVG <use> instance trees (#13180)
			//
			// Support: Firefox <=42
			// Avoid non-left-click in FF but don't block IE radio events (#3861, gh-2343)
			if ( delegateCount && cur.nodeType &&
				( event.type !== "click" || isNaN( event.button ) || event.button < 1 ) ) {

				for ( ; cur !== this; cur = cur.parentNode || this ) {

					// Don't check non-elements (#13208)
					// Don't process clicks on disabled elements (#6911, #8165, #11382, #11764)
					if ( cur.nodeType === 1 && ( cur.disabled !== true || event.type !== "click" ) ) {
						matches = [];
						for ( i = 0; i < delegateCount; i++ ) {
							handleObj = handlers[ i ];

							// Don't conflict with Object.prototype properties (#13203)
							sel = handleObj.selector + " ";

							if ( matches[ sel ] === undefined ) {
								matches[ sel ] = handleObj.needsContext ?
									jQuery( sel, this ).index( cur ) > -1 :
									jQuery.find( sel, this, null, [ cur ] ).length;
							}
							if ( matches[ sel ] ) {
								matches.push( handleObj );
							}
						}
						if ( matches.length ) {
							handlerQueue.push( { elem: cur, handlers: matches } );
						}
					}
				}
			}

			// Add the remaining (directly-bound) handlers
			if ( delegateCount < handlers.length ) {
				handlerQueue.push( { elem: this, handlers: handlers.slice( delegateCount ) } );
			}

			return handlerQueue;
		},

		addProp: function( name, hook ) {
			Object.defineProperty( jQuery.Event.prototype, name, {
				enumerable: true,
				configurable: true,

				get: jQuery.isFunction( hook ) ?
					function() {
						if ( this.originalEvent ) {
								return hook( this.originalEvent );
						}
					} :
					function() {
						if ( this.originalEvent ) {
								return this.originalEvent[ name ];
						}
					},

				set: function( value ) {
					Object.defineProperty( this, name, {
						enumerable: true,
						configurable: true,
						writable: true,
						value: value
					} );
				}
			} );
		},

		fix: function( originalEvent ) {
			return originalEvent[ jQuery.expando ] ?
				originalEvent :
				new jQuery.Event( originalEvent );
		},

		special: {
			load: {

				// Prevent triggered image.load events from bubbling to window.load
				noBubble: true
			},
			focus: {

				// Fire native event if possible so blur/focus sequence is correct
				trigger: function() {
					if ( this !== safeActiveElement() && this.focus ) {
						this.focus();
						return false;
					}
				},
				delegateType: "focusin"
			},
			blur: {
				trigger: function() {
					if ( this === safeActiveElement() && this.blur ) {
						this.blur();
						return false;
					}
				},
				delegateType: "focusout"
			},
			click: {

				// For checkbox, fire native event so checked state will be right
				trigger: function() {
					if ( this.type === "checkbox" && this.click && jQuery.nodeName( this, "input" ) ) {
						this.click();
						return false;
					}
				},

				// For cross-browser consistency, don't fire native .click() on links
				_default: function( event ) {
					return jQuery.nodeName( event.target, "a" );
				}
			},

			beforeunload: {
				postDispatch: function( event ) {

					// Support: Firefox 20+
					// Firefox doesn't alert if the returnValue field is not set.
					if ( event.result !== undefined && event.originalEvent ) {
						event.originalEvent.returnValue = event.result;
					}
				}
			}
		}
	};

	jQuery.removeEvent = function( elem, type, handle ) {

		// This "if" is needed for plain objects
		if ( elem.removeEventListener ) {
			elem.removeEventListener( type, handle );
		}
	};

	jQuery.Event = function( src, props ) {

		// Allow instantiation without the 'new' keyword
		if ( !( this instanceof jQuery.Event ) ) {
			return new jQuery.Event( src, props );
		}

		// Event object
		if ( src && src.type ) {
			this.originalEvent = src;
			this.type = src.type;

			// Events bubbling up the document may have been marked as prevented
			// by a handler lower down the tree; reflect the correct value.
			this.isDefaultPrevented = src.defaultPrevented ||
					src.defaultPrevented === undefined &&

					// Support: Android <=2.3 only
					src.returnValue === false ?
				returnTrue :
				returnFalse;

			// Create target properties
			// Support: Safari <=6 - 7 only
			// Target should not be a text node (#504, #13143)
			this.target = ( src.target && src.target.nodeType === 3 ) ?
				src.target.parentNode :
				src.target;

			this.currentTarget = src.currentTarget;
			this.relatedTarget = src.relatedTarget;

		// Event type
		} else {
			this.type = src;
		}

		// Put explicitly provided properties onto the event object
		if ( props ) {
			jQuery.extend( this, props );
		}

		// Create a timestamp if incoming event doesn't have one
		this.timeStamp = src && src.timeStamp || jQuery.now();

		// Mark it as fixed
		this[ jQuery.expando ] = true;
	};

	// jQuery.Event is based on DOM3 Events as specified by the ECMAScript Language Binding
	// https://www.w3.org/TR/2003/WD-DOM-Level-3-Events-20030331/ecma-script-binding.html
	jQuery.Event.prototype = {
		constructor: jQuery.Event,
		isDefaultPrevented: returnFalse,
		isPropagationStopped: returnFalse,
		isImmediatePropagationStopped: returnFalse,
		isSimulated: false,

		preventDefault: function() {
			var e = this.originalEvent;

			this.isDefaultPrevented = returnTrue;

			if ( e && !this.isSimulated ) {
				e.preventDefault();
			}
		},
		stopPropagation: function() {
			var e = this.originalEvent;

			this.isPropagationStopped = returnTrue;

			if ( e && !this.isSimulated ) {
				e.stopPropagation();
			}
		},
		stopImmediatePropagation: function() {
			var e = this.originalEvent;

			this.isImmediatePropagationStopped = returnTrue;

			if ( e && !this.isSimulated ) {
				e.stopImmediatePropagation();
			}

			this.stopPropagation();
		}
	};

	// Includes all common event props including KeyEvent and MouseEvent specific props
	jQuery.each( {
		altKey: true,
		bubbles: true,
		cancelable: true,
		changedTouches: true,
		ctrlKey: true,
		detail: true,
		eventPhase: true,
		metaKey: true,
		pageX: true,
		pageY: true,
		shiftKey: true,
		view: true,
		"char": true,
		charCode: true,
		key: true,
		keyCode: true,
		button: true,
		buttons: true,
		clientX: true,
		clientY: true,
		offsetX: true,
		offsetY: true,
		pointerId: true,
		pointerType: true,
		screenX: true,
		screenY: true,
		targetTouches: true,
		toElement: true,
		touches: true,

		which: function( event ) {
			var button = event.button;

			// Add which for key events
			if ( event.which == null && rkeyEvent.test( event.type ) ) {
				return event.charCode != null ? event.charCode : event.keyCode;
			}

			// Add which for click: 1 === left; 2 === middle; 3 === right
			if ( !event.which && button !== undefined && rmouseEvent.test( event.type ) ) {
				return ( button & 1 ? 1 : ( button & 2 ? 3 : ( button & 4 ? 2 : 0 ) ) );
			}

			return event.which;
		}
	}, jQuery.event.addProp );

	// Create mouseenter/leave events using mouseover/out and event-time checks
	// so that event delegation works in jQuery.
	// Do the same for pointerenter/pointerleave and pointerover/pointerout
	//
	// Support: Safari 7 only
	// Safari sends mouseenter too often; see:
	// https://bugs.chromium.org/p/chromium/issues/detail?id=470258
	// for the description of the bug (it existed in older Chrome versions as well).
	jQuery.each( {
		mouseenter: "mouseover",
		mouseleave: "mouseout",
		pointerenter: "pointerover",
		pointerleave: "pointerout"
	}, function( orig, fix ) {
		jQuery.event.special[ orig ] = {
			delegateType: fix,
			bindType: fix,

			handle: function( event ) {
				var ret,
					target = this,
					related = event.relatedTarget,
					handleObj = event.handleObj;

				// For mouseenter/leave call the handler if related is outside the target.
				// NB: No relatedTarget if the mouse left/entered the browser window
				if ( !related || ( related !== target && !jQuery.contains( target, related ) ) ) {
					event.type = handleObj.origType;
					ret = handleObj.handler.apply( this, arguments );
					event.type = fix;
				}
				return ret;
			}
		};
	} );

	jQuery.fn.extend( {

		on: function( types, selector, data, fn ) {
			return on( this, types, selector, data, fn );
		},
		one: function( types, selector, data, fn ) {
			return on( this, types, selector, data, fn, 1 );
		},
		off: function( types, selector, fn ) {
			var handleObj, type;
			if ( types && types.preventDefault && types.handleObj ) {

				// ( event )  dispatched jQuery.Event
				handleObj = types.handleObj;
				jQuery( types.delegateTarget ).off(
					handleObj.namespace ?
						handleObj.origType + "." + handleObj.namespace :
						handleObj.origType,
					handleObj.selector,
					handleObj.handler
				);
				return this;
			}
			if ( typeof types === "object" ) {

				// ( types-object [, selector] )
				for ( type in types ) {
					this.off( type, selector, types[ type ] );
				}
				return this;
			}
			if ( selector === false || typeof selector === "function" ) {

				// ( types [, fn] )
				fn = selector;
				selector = undefined;
			}
			if ( fn === false ) {
				fn = returnFalse;
			}
			return this.each( function() {
				jQuery.event.remove( this, types, fn, selector );
			} );
		}
	} );


	var

		/* eslint-disable max-len */

		// See https://github.com/eslint/eslint/issues/3229
		rxhtmlTag = /<(?!area|br|col|embed|hr|img|input|link|meta|param)(([a-z][^\/\0>\x20\t\r\n\f]*)[^>]*)\/>/gi,

		/* eslint-enable */

		// Support: IE <=10 - 11, Edge 12 - 13
		// In IE/Edge using regex groups here causes severe slowdowns.
		// See https://connect.microsoft.com/IE/feedback/details/1736512/
		rnoInnerhtml = /<script|<style|<link/i,

		// checked="checked" or checked
		rchecked = /checked\s*(?:[^=]|=\s*.checked.)/i,
		rscriptTypeMasked = /^true\/(.*)/,
		rcleanScript = /^\s*<!(?:\[CDATA\[|--)|(?:\]\]|--)>\s*$/g;

	function manipulationTarget( elem, content ) {
		if ( jQuery.nodeName( elem, "table" ) &&
			jQuery.nodeName( content.nodeType !== 11 ? content : content.firstChild, "tr" ) ) {

			return elem.getElementsByTagName( "tbody" )[ 0 ] || elem;
		}

		return elem;
	}

	// Replace/restore the type attribute of script elements for safe DOM manipulation
	function disableScript( elem ) {
		elem.type = ( elem.getAttribute( "type" ) !== null ) + "/" + elem.type;
		return elem;
	}
	function restoreScript( elem ) {
		var match = rscriptTypeMasked.exec( elem.type );

		if ( match ) {
			elem.type = match[ 1 ];
		} else {
			elem.removeAttribute( "type" );
		}

		return elem;
	}

	function cloneCopyEvent( src, dest ) {
		var i, l, type, pdataOld, pdataCur, udataOld, udataCur, events;

		if ( dest.nodeType !== 1 ) {
			return;
		}

		// 1. Copy private data: events, handlers, etc.
		if ( dataPriv.hasData( src ) ) {
			pdataOld = dataPriv.access( src );
			pdataCur = dataPriv.set( dest, pdataOld );
			events = pdataOld.events;

			if ( events ) {
				delete pdataCur.handle;
				pdataCur.events = {};

				for ( type in events ) {
					for ( i = 0, l = events[ type ].length; i < l; i++ ) {
						jQuery.event.add( dest, type, events[ type ][ i ] );
					}
				}
			}
		}

		// 2. Copy user data
		if ( dataUser.hasData( src ) ) {
			udataOld = dataUser.access( src );
			udataCur = jQuery.extend( {}, udataOld );

			dataUser.set( dest, udataCur );
		}
	}

	// Fix IE bugs, see support tests
	function fixInput( src, dest ) {
		var nodeName = dest.nodeName.toLowerCase();

		// Fails to persist the checked state of a cloned checkbox or radio button.
		if ( nodeName === "input" && rcheckableType.test( src.type ) ) {
			dest.checked = src.checked;

		// Fails to return the selected option to the default selected state when cloning options
		} else if ( nodeName === "input" || nodeName === "textarea" ) {
			dest.defaultValue = src.defaultValue;
		}
	}

	function domManip( collection, args, callback, ignored ) {

		// Flatten any nested arrays
		args = concat.apply( [], args );

		var fragment, first, scripts, hasScripts, node, doc,
			i = 0,
			l = collection.length,
			iNoClone = l - 1,
			value = args[ 0 ],
			isFunction = jQuery.isFunction( value );

		// We can't cloneNode fragments that contain checked, in WebKit
		if ( isFunction ||
				( l > 1 && typeof value === "string" &&
					!support.checkClone && rchecked.test( value ) ) ) {
			return collection.each( function( index ) {
				var self = collection.eq( index );
				if ( isFunction ) {
					args[ 0 ] = value.call( this, index, self.html() );
				}
				domManip( self, args, callback, ignored );
			} );
		}

		if ( l ) {
			fragment = buildFragment( args, collection[ 0 ].ownerDocument, false, collection, ignored );
			first = fragment.firstChild;

			if ( fragment.childNodes.length === 1 ) {
				fragment = first;
			}

			// Require either new content or an interest in ignored elements to invoke the callback
			if ( first || ignored ) {
				scripts = jQuery.map( getAll( fragment, "script" ), disableScript );
				hasScripts = scripts.length;

				// Use the original fragment for the last item
				// instead of the first because it can end up
				// being emptied incorrectly in certain situations (#8070).
				for ( ; i < l; i++ ) {
					node = fragment;

					if ( i !== iNoClone ) {
						node = jQuery.clone( node, true, true );

						// Keep references to cloned scripts for later restoration
						if ( hasScripts ) {

							// Support: Android <=4.0 only, PhantomJS 1 only
							// push.apply(_, arraylike) throws on ancient WebKit
							jQuery.merge( scripts, getAll( node, "script" ) );
						}
					}

					callback.call( collection[ i ], node, i );
				}

				if ( hasScripts ) {
					doc = scripts[ scripts.length - 1 ].ownerDocument;

					// Reenable scripts
					jQuery.map( scripts, restoreScript );

					// Evaluate executable scripts on first document insertion
					for ( i = 0; i < hasScripts; i++ ) {
						node = scripts[ i ];
						if ( rscriptType.test( node.type || "" ) &&
							!dataPriv.access( node, "globalEval" ) &&
							jQuery.contains( doc, node ) ) {

							if ( node.src ) {

								// Optional AJAX dependency, but won't run scripts if not present
								if ( jQuery._evalUrl ) {
									jQuery._evalUrl( node.src );
								}
							} else {
								DOMEval( node.textContent.replace( rcleanScript, "" ), doc );
							}
						}
					}
				}
			}
		}

		return collection;
	}

	function remove( elem, selector, keepData ) {
		var node,
			nodes = selector ? jQuery.filter( selector, elem ) : elem,
			i = 0;

		for ( ; ( node = nodes[ i ] ) != null; i++ ) {
			if ( !keepData && node.nodeType === 1 ) {
				jQuery.cleanData( getAll( node ) );
			}

			if ( node.parentNode ) {
				if ( keepData && jQuery.contains( node.ownerDocument, node ) ) {
					setGlobalEval( getAll( node, "script" ) );
				}
				node.parentNode.removeChild( node );
			}
		}

		return elem;
	}

	jQuery.extend( {
		htmlPrefilter: function( html ) {
			return html.replace( rxhtmlTag, "<$1></$2>" );
		},

		clone: function( elem, dataAndEvents, deepDataAndEvents ) {
			var i, l, srcElements, destElements,
				clone = elem.cloneNode( true ),
				inPage = jQuery.contains( elem.ownerDocument, elem );

			// Fix IE cloning issues
			if ( !support.noCloneChecked && ( elem.nodeType === 1 || elem.nodeType === 11 ) &&
					!jQuery.isXMLDoc( elem ) ) {

				// We eschew Sizzle here for performance reasons: https://jsperf.com/getall-vs-sizzle/2
				destElements = getAll( clone );
				srcElements = getAll( elem );

				for ( i = 0, l = srcElements.length; i < l; i++ ) {
					fixInput( srcElements[ i ], destElements[ i ] );
				}
			}

			// Copy the events from the original to the clone
			if ( dataAndEvents ) {
				if ( deepDataAndEvents ) {
					srcElements = srcElements || getAll( elem );
					destElements = destElements || getAll( clone );

					for ( i = 0, l = srcElements.length; i < l; i++ ) {
						cloneCopyEvent( srcElements[ i ], destElements[ i ] );
					}
				} else {
					cloneCopyEvent( elem, clone );
				}
			}

			// Preserve script evaluation history
			destElements = getAll( clone, "script" );
			if ( destElements.length > 0 ) {
				setGlobalEval( destElements, !inPage && getAll( elem, "script" ) );
			}

			// Return the cloned set
			return clone;
		},

		cleanData: function( elems ) {
			var data, elem, type,
				special = jQuery.event.special,
				i = 0;

			for ( ; ( elem = elems[ i ] ) !== undefined; i++ ) {
				if ( acceptData( elem ) ) {
					if ( ( data = elem[ dataPriv.expando ] ) ) {
						if ( data.events ) {
							for ( type in data.events ) {
								if ( special[ type ] ) {
									jQuery.event.remove( elem, type );

								// This is a shortcut to avoid jQuery.event.remove's overhead
								} else {
									jQuery.removeEvent( elem, type, data.handle );
								}
							}
						}

						// Support: Chrome <=35 - 45+
						// Assign undefined instead of using delete, see Data#remove
						elem[ dataPriv.expando ] = undefined;
					}
					if ( elem[ dataUser.expando ] ) {

						// Support: Chrome <=35 - 45+
						// Assign undefined instead of using delete, see Data#remove
						elem[ dataUser.expando ] = undefined;
					}
				}
			}
		}
	} );

	jQuery.fn.extend( {
		detach: function( selector ) {
			return remove( this, selector, true );
		},

		remove: function( selector ) {
			return remove( this, selector );
		},

		text: function( value ) {
			return access( this, function( value ) {
				return value === undefined ?
					jQuery.text( this ) :
					this.empty().each( function() {
						if ( this.nodeType === 1 || this.nodeType === 11 || this.nodeType === 9 ) {
							this.textContent = value;
						}
					} );
			}, null, value, arguments.length );
		},

		append: function() {
			return domManip( this, arguments, function( elem ) {
				if ( this.nodeType === 1 || this.nodeType === 11 || this.nodeType === 9 ) {
					var target = manipulationTarget( this, elem );
					target.appendChild( elem );
				}
			} );
		},

		prepend: function() {
			return domManip( this, arguments, function( elem ) {
				if ( this.nodeType === 1 || this.nodeType === 11 || this.nodeType === 9 ) {
					var target = manipulationTarget( this, elem );
					target.insertBefore( elem, target.firstChild );
				}
			} );
		},

		before: function() {
			return domManip( this, arguments, function( elem ) {
				if ( this.parentNode ) {
					this.parentNode.insertBefore( elem, this );
				}
			} );
		},

		after: function() {
			return domManip( this, arguments, function( elem ) {
				if ( this.parentNode ) {
					this.parentNode.insertBefore( elem, this.nextSibling );
				}
			} );
		},

		empty: function() {
			var elem,
				i = 0;

			for ( ; ( elem = this[ i ] ) != null; i++ ) {
				if ( elem.nodeType === 1 ) {

					// Prevent memory leaks
					jQuery.cleanData( getAll( elem, false ) );

					// Remove any remaining nodes
					elem.textContent = "";
				}
			}

			return this;
		},

		clone: function( dataAndEvents, deepDataAndEvents ) {
			dataAndEvents = dataAndEvents == null ? false : dataAndEvents;
			deepDataAndEvents = deepDataAndEvents == null ? dataAndEvents : deepDataAndEvents;

			return this.map( function() {
				return jQuery.clone( this, dataAndEvents, deepDataAndEvents );
			} );
		},

		html: function( value ) {
			return access( this, function( value ) {
				var elem = this[ 0 ] || {},
					i = 0,
					l = this.length;

				if ( value === undefined && elem.nodeType === 1 ) {
					return elem.innerHTML;
				}

				// See if we can take a shortcut and just use innerHTML
				if ( typeof value === "string" && !rnoInnerhtml.test( value ) &&
					!wrapMap[ ( rtagName.exec( value ) || [ "", "" ] )[ 1 ].toLowerCase() ] ) {

					value = jQuery.htmlPrefilter( value );

					try {
						for ( ; i < l; i++ ) {
							elem = this[ i ] || {};

							// Remove element nodes and prevent memory leaks
							if ( elem.nodeType === 1 ) {
								jQuery.cleanData( getAll( elem, false ) );
								elem.innerHTML = value;
							}
						}

						elem = 0;

					// If using innerHTML throws an exception, use the fallback method
					} catch ( e ) {}
				}

				if ( elem ) {
					this.empty().append( value );
				}
			}, null, value, arguments.length );
		},

		replaceWith: function() {
			var ignored = [];

			// Make the changes, replacing each non-ignored context element with the new content
			return domManip( this, arguments, function( elem ) {
				var parent = this.parentNode;

				if ( jQuery.inArray( this, ignored ) < 0 ) {
					jQuery.cleanData( getAll( this ) );
					if ( parent ) {
						parent.replaceChild( elem, this );
					}
				}

			// Force callback invocation
			}, ignored );
		}
	} );

	jQuery.each( {
		appendTo: "append",
		prependTo: "prepend",
		insertBefore: "before",
		insertAfter: "after",
		replaceAll: "replaceWith"
	}, function( name, original ) {
		jQuery.fn[ name ] = function( selector ) {
			var elems,
				ret = [],
				insert = jQuery( selector ),
				last = insert.length - 1,
				i = 0;

			for ( ; i <= last; i++ ) {
				elems = i === last ? this : this.clone( true );
				jQuery( insert[ i ] )[ original ]( elems );

				// Support: Android <=4.0 only, PhantomJS 1 only
				// .get() because push.apply(_, arraylike) throws on ancient WebKit
				push.apply( ret, elems.get() );
			}

			return this.pushStack( ret );
		};
	} );
	var rmargin = ( /^margin/ );

	var rnumnonpx = new RegExp( "^(" + pnum + ")(?!px)[a-z%]+$", "i" );

	var getStyles = function( elem ) {

			// Support: IE <=11 only, Firefox <=30 (#15098, #14150)
			// IE throws on elements created in popups
			// FF meanwhile throws on frame elements through "defaultView.getComputedStyle"
			var view = elem.ownerDocument.defaultView;

			if ( !view || !view.opener ) {
				view = window;
			}

			return view.getComputedStyle( elem );
		};



	( function() {

		// Executing both pixelPosition & boxSizingReliable tests require only one layout
		// so they're executed at the same time to save the second computation.
		function computeStyleTests() {

			// This is a singleton, we need to execute it only once
			if ( !div ) {
				return;
			}

			div.style.cssText =
				"box-sizing:border-box;" +
				"position:relative;display:block;" +
				"margin:auto;border:1px;padding:1px;" +
				"top:1%;width:50%";
			div.innerHTML = "";
			documentElement.appendChild( container );

			var divStyle = window.getComputedStyle( div );
			pixelPositionVal = divStyle.top !== "1%";

			// Support: Android 4.0 - 4.3 only, Firefox <=3 - 44
			reliableMarginLeftVal = divStyle.marginLeft === "2px";
			boxSizingReliableVal = divStyle.width === "4px";

			// Support: Android 4.0 - 4.3 only
			// Some styles come back with percentage values, even though they shouldn't
			div.style.marginRight = "50%";
			pixelMarginRightVal = divStyle.marginRight === "4px";

			documentElement.removeChild( container );

			// Nullify the div so it wouldn't be stored in the memory and
			// it will also be a sign that checks already performed
			div = null;
		}

		var pixelPositionVal, boxSizingReliableVal, pixelMarginRightVal, reliableMarginLeftVal,
			container = document.createElement( "div" ),
			div = document.createElement( "div" );

		// Finish early in limited (non-browser) environments
		if ( !div.style ) {
			return;
		}

		// Support: IE <=9 - 11 only
		// Style of cloned element affects source element cloned (#8908)
		div.style.backgroundClip = "content-box";
		div.cloneNode( true ).style.backgroundClip = "";
		support.clearCloneStyle = div.style.backgroundClip === "content-box";

		container.style.cssText = "border:0;width:8px;height:0;top:0;left:-9999px;" +
			"padding:0;margin-top:1px;position:absolute";
		container.appendChild( div );

		jQuery.extend( support, {
			pixelPosition: function() {
				computeStyleTests();
				return pixelPositionVal;
			},
			boxSizingReliable: function() {
				computeStyleTests();
				return boxSizingReliableVal;
			},
			pixelMarginRight: function() {
				computeStyleTests();
				return pixelMarginRightVal;
			},
			reliableMarginLeft: function() {
				computeStyleTests();
				return reliableMarginLeftVal;
			}
		} );
	} )();


	function curCSS( elem, name, computed ) {
		var width, minWidth, maxWidth, ret,
			style = elem.style;

		computed = computed || getStyles( elem );

		// Support: IE <=9 only
		// getPropertyValue is only needed for .css('filter') (#12537)
		if ( computed ) {
			ret = computed.getPropertyValue( name ) || computed[ name ];

			if ( ret === "" && !jQuery.contains( elem.ownerDocument, elem ) ) {
				ret = jQuery.style( elem, name );
			}

			// A tribute to the "awesome hack by Dean Edwards"
			// Android Browser returns percentage for some values,
			// but width seems to be reliably pixels.
			// This is against the CSSOM draft spec:
			// https://drafts.csswg.org/cssom/#resolved-values
			if ( !support.pixelMarginRight() && rnumnonpx.test( ret ) && rmargin.test( name ) ) {

				// Remember the original values
				width = style.width;
				minWidth = style.minWidth;
				maxWidth = style.maxWidth;

				// Put in the new values to get a computed value out
				style.minWidth = style.maxWidth = style.width = ret;
				ret = computed.width;

				// Revert the changed values
				style.width = width;
				style.minWidth = minWidth;
				style.maxWidth = maxWidth;
			}
		}

		return ret !== undefined ?

			// Support: IE <=9 - 11 only
			// IE returns zIndex value as an integer.
			ret + "" :
			ret;
	}


	function addGetHookIf( conditionFn, hookFn ) {

		// Define the hook, we'll check on the first run if it's really needed.
		return {
			get: function() {
				if ( conditionFn() ) {

					// Hook not needed (or it's not possible to use it due
					// to missing dependency), remove it.
					delete this.get;
					return;
				}

				// Hook needed; redefine it so that the support test is not executed again.
				return ( this.get = hookFn ).apply( this, arguments );
			}
		};
	}


	var

		// Swappable if display is none or starts with table
		// except "table", "table-cell", or "table-caption"
		// See here for display values: https://developer.mozilla.org/en-US/docs/CSS/display
		rdisplayswap = /^(none|table(?!-c[ea]).+)/,
		cssShow = { position: "absolute", visibility: "hidden", display: "block" },
		cssNormalTransform = {
			letterSpacing: "0",
			fontWeight: "400"
		},

		cssPrefixes = [ "Webkit", "Moz", "ms" ],
		emptyStyle = document.createElement( "div" ).style;

	// Return a css property mapped to a potentially vendor prefixed property
	function vendorPropName( name ) {

		// Shortcut for names that are not vendor prefixed
		if ( name in emptyStyle ) {
			return name;
		}

		// Check for vendor prefixed names
		var capName = name[ 0 ].toUpperCase() + name.slice( 1 ),
			i = cssPrefixes.length;

		while ( i-- ) {
			name = cssPrefixes[ i ] + capName;
			if ( name in emptyStyle ) {
				return name;
			}
		}
	}

	function setPositiveNumber( elem, value, subtract ) {

		// Any relative (+/-) values have already been
		// normalized at this point
		var matches = rcssNum.exec( value );
		return matches ?

			// Guard against undefined "subtract", e.g., when used as in cssHooks
			Math.max( 0, matches[ 2 ] - ( subtract || 0 ) ) + ( matches[ 3 ] || "px" ) :
			value;
	}

	function augmentWidthOrHeight( elem, name, extra, isBorderBox, styles ) {
		var i = extra === ( isBorderBox ? "border" : "content" ) ?

			// If we already have the right measurement, avoid augmentation
			4 :

			// Otherwise initialize for horizontal or vertical properties
			name === "width" ? 1 : 0,

			val = 0;

		for ( ; i < 4; i += 2 ) {

			// Both box models exclude margin, so add it if we want it
			if ( extra === "margin" ) {
				val += jQuery.css( elem, extra + cssExpand[ i ], true, styles );
			}

			if ( isBorderBox ) {

				// border-box includes padding, so remove it if we want content
				if ( extra === "content" ) {
					val -= jQuery.css( elem, "padding" + cssExpand[ i ], true, styles );
				}

				// At this point, extra isn't border nor margin, so remove border
				if ( extra !== "margin" ) {
					val -= jQuery.css( elem, "border" + cssExpand[ i ] + "Width", true, styles );
				}
			} else {

				// At this point, extra isn't content, so add padding
				val += jQuery.css( elem, "padding" + cssExpand[ i ], true, styles );

				// At this point, extra isn't content nor padding, so add border
				if ( extra !== "padding" ) {
					val += jQuery.css( elem, "border" + cssExpand[ i ] + "Width", true, styles );
				}
			}
		}

		return val;
	}

	function getWidthOrHeight( elem, name, extra ) {

		// Start with offset property, which is equivalent to the border-box value
		var val,
			valueIsBorderBox = true,
			styles = getStyles( elem ),
			isBorderBox = jQuery.css( elem, "boxSizing", false, styles ) === "border-box";

		// Support: IE <=11 only
		// Running getBoundingClientRect on a disconnected node
		// in IE throws an error.
		if ( elem.getClientRects().length ) {
			val = elem.getBoundingClientRect()[ name ];
		}

		// Some non-html elements return undefined for offsetWidth, so check for null/undefined
		// svg - https://bugzilla.mozilla.org/show_bug.cgi?id=649285
		// MathML - https://bugzilla.mozilla.org/show_bug.cgi?id=491668
		if ( val <= 0 || val == null ) {

			// Fall back to computed then uncomputed css if necessary
			val = curCSS( elem, name, styles );
			if ( val < 0 || val == null ) {
				val = elem.style[ name ];
			}

			// Computed unit is not pixels. Stop here and return.
			if ( rnumnonpx.test( val ) ) {
				return val;
			}

			// Check for style in case a browser which returns unreliable values
			// for getComputedStyle silently falls back to the reliable elem.style
			valueIsBorderBox = isBorderBox &&
				( support.boxSizingReliable() || val === elem.style[ name ] );

			// Normalize "", auto, and prepare for extra
			val = parseFloat( val ) || 0;
		}

		// Use the active box-sizing model to add/subtract irrelevant styles
		return ( val +
			augmentWidthOrHeight(
				elem,
				name,
				extra || ( isBorderBox ? "border" : "content" ),
				valueIsBorderBox,
				styles
			)
		) + "px";
	}

	jQuery.extend( {

		// Add in style property hooks for overriding the default
		// behavior of getting and setting a style property
		cssHooks: {
			opacity: {
				get: function( elem, computed ) {
					if ( computed ) {

						// We should always get a number back from opacity
						var ret = curCSS( elem, "opacity" );
						return ret === "" ? "1" : ret;
					}
				}
			}
		},

		// Don't automatically add "px" to these possibly-unitless properties
		cssNumber: {
			"animationIterationCount": true,
			"columnCount": true,
			"fillOpacity": true,
			"flexGrow": true,
			"flexShrink": true,
			"fontWeight": true,
			"lineHeight": true,
			"opacity": true,
			"order": true,
			"orphans": true,
			"widows": true,
			"zIndex": true,
			"zoom": true
		},

		// Add in properties whose names you wish to fix before
		// setting or getting the value
		cssProps: {
			"float": "cssFloat"
		},

		// Get and set the style property on a DOM Node
		style: function( elem, name, value, extra ) {

			// Don't set styles on text and comment nodes
			if ( !elem || elem.nodeType === 3 || elem.nodeType === 8 || !elem.style ) {
				return;
			}

			// Make sure that we're working with the right name
			var ret, type, hooks,
				origName = jQuery.camelCase( name ),
				style = elem.style;

			name = jQuery.cssProps[ origName ] ||
				( jQuery.cssProps[ origName ] = vendorPropName( origName ) || origName );

			// Gets hook for the prefixed version, then unprefixed version
			hooks = jQuery.cssHooks[ name ] || jQuery.cssHooks[ origName ];

			// Check if we're setting a value
			if ( value !== undefined ) {
				type = typeof value;

				// Convert "+=" or "-=" to relative numbers (#7345)
				if ( type === "string" && ( ret = rcssNum.exec( value ) ) && ret[ 1 ] ) {
					value = adjustCSS( elem, name, ret );

					// Fixes bug #9237
					type = "number";
				}

				// Make sure that null and NaN values aren't set (#7116)
				if ( value == null || value !== value ) {
					return;
				}

				// If a number was passed in, add the unit (except for certain CSS properties)
				if ( type === "number" ) {
					value += ret && ret[ 3 ] || ( jQuery.cssNumber[ origName ] ? "" : "px" );
				}

				// background-* props affect original clone's values
				if ( !support.clearCloneStyle && value === "" && name.indexOf( "background" ) === 0 ) {
					style[ name ] = "inherit";
				}

				// If a hook was provided, use that value, otherwise just set the specified value
				if ( !hooks || !( "set" in hooks ) ||
					( value = hooks.set( elem, value, extra ) ) !== undefined ) {

					style[ name ] = value;
				}

			} else {

				// If a hook was provided get the non-computed value from there
				if ( hooks && "get" in hooks &&
					( ret = hooks.get( elem, false, extra ) ) !== undefined ) {

					return ret;
				}

				// Otherwise just get the value from the style object
				return style[ name ];
			}
		},

		css: function( elem, name, extra, styles ) {
			var val, num, hooks,
				origName = jQuery.camelCase( name );

			// Make sure that we're working with the right name
			name = jQuery.cssProps[ origName ] ||
				( jQuery.cssProps[ origName ] = vendorPropName( origName ) || origName );

			// Try prefixed name followed by the unprefixed name
			hooks = jQuery.cssHooks[ name ] || jQuery.cssHooks[ origName ];

			// If a hook was provided get the computed value from there
			if ( hooks && "get" in hooks ) {
				val = hooks.get( elem, true, extra );
			}

			// Otherwise, if a way to get the computed value exists, use that
			if ( val === undefined ) {
				val = curCSS( elem, name, styles );
			}

			// Convert "normal" to computed value
			if ( val === "normal" && name in cssNormalTransform ) {
				val = cssNormalTransform[ name ];
			}

			// Make numeric if forced or a qualifier was provided and val looks numeric
			if ( extra === "" || extra ) {
				num = parseFloat( val );
				return extra === true || isFinite( num ) ? num || 0 : val;
			}
			return val;
		}
	} );

	jQuery.each( [ "height", "width" ], function( i, name ) {
		jQuery.cssHooks[ name ] = {
			get: function( elem, computed, extra ) {
				if ( computed ) {

					// Certain elements can have dimension info if we invisibly show them
					// but it must have a current display style that would benefit
					return rdisplayswap.test( jQuery.css( elem, "display" ) ) &&

						// Support: Safari 8+
						// Table columns in Safari have non-zero offsetWidth & zero
						// getBoundingClientRect().width unless display is changed.
						// Support: IE <=11 only
						// Running getBoundingClientRect on a disconnected node
						// in IE throws an error.
						( !elem.getClientRects().length || !elem.getBoundingClientRect().width ) ?
							swap( elem, cssShow, function() {
								return getWidthOrHeight( elem, name, extra );
							} ) :
							getWidthOrHeight( elem, name, extra );
				}
			},

			set: function( elem, value, extra ) {
				var matches,
					styles = extra && getStyles( elem ),
					subtract = extra && augmentWidthOrHeight(
						elem,
						name,
						extra,
						jQuery.css( elem, "boxSizing", false, styles ) === "border-box",
						styles
					);

				// Convert to pixels if value adjustment is needed
				if ( subtract && ( matches = rcssNum.exec( value ) ) &&
					( matches[ 3 ] || "px" ) !== "px" ) {

					elem.style[ name ] = value;
					value = jQuery.css( elem, name );
				}

				return setPositiveNumber( elem, value, subtract );
			}
		};
	} );

	jQuery.cssHooks.marginLeft = addGetHookIf( support.reliableMarginLeft,
		function( elem, computed ) {
			if ( computed ) {
				return ( parseFloat( curCSS( elem, "marginLeft" ) ) ||
					elem.getBoundingClientRect().left -
						swap( elem, { marginLeft: 0 }, function() {
							return elem.getBoundingClientRect().left;
						} )
					) + "px";
			}
		}
	);

	// These hooks are used by animate to expand properties
	jQuery.each( {
		margin: "",
		padding: "",
		border: "Width"
	}, function( prefix, suffix ) {
		jQuery.cssHooks[ prefix + suffix ] = {
			expand: function( value ) {
				var i = 0,
					expanded = {},

					// Assumes a single number if not a string
					parts = typeof value === "string" ? value.split( " " ) : [ value ];

				for ( ; i < 4; i++ ) {
					expanded[ prefix + cssExpand[ i ] + suffix ] =
						parts[ i ] || parts[ i - 2 ] || parts[ 0 ];
				}

				return expanded;
			}
		};

		if ( !rmargin.test( prefix ) ) {
			jQuery.cssHooks[ prefix + suffix ].set = setPositiveNumber;
		}
	} );

	jQuery.fn.extend( {
		css: function( name, value ) {
			return access( this, function( elem, name, value ) {
				var styles, len,
					map = {},
					i = 0;

				if ( jQuery.isArray( name ) ) {
					styles = getStyles( elem );
					len = name.length;

					for ( ; i < len; i++ ) {
						map[ name[ i ] ] = jQuery.css( elem, name[ i ], false, styles );
					}

					return map;
				}

				return value !== undefined ?
					jQuery.style( elem, name, value ) :
					jQuery.css( elem, name );
			}, name, value, arguments.length > 1 );
		}
	} );


	function Tween( elem, options, prop, end, easing ) {
		return new Tween.prototype.init( elem, options, prop, end, easing );
	}
	jQuery.Tween = Tween;

	Tween.prototype = {
		constructor: Tween,
		init: function( elem, options, prop, end, easing, unit ) {
			this.elem = elem;
			this.prop = prop;
			this.easing = easing || jQuery.easing._default;
			this.options = options;
			this.start = this.now = this.cur();
			this.end = end;
			this.unit = unit || ( jQuery.cssNumber[ prop ] ? "" : "px" );
		},
		cur: function() {
			var hooks = Tween.propHooks[ this.prop ];

			return hooks && hooks.get ?
				hooks.get( this ) :
				Tween.propHooks._default.get( this );
		},
		run: function( percent ) {
			var eased,
				hooks = Tween.propHooks[ this.prop ];

			if ( this.options.duration ) {
				this.pos = eased = jQuery.easing[ this.easing ](
					percent, this.options.duration * percent, 0, 1, this.options.duration
				);
			} else {
				this.pos = eased = percent;
			}
			this.now = ( this.end - this.start ) * eased + this.start;

			if ( this.options.step ) {
				this.options.step.call( this.elem, this.now, this );
			}

			if ( hooks && hooks.set ) {
				hooks.set( this );
			} else {
				Tween.propHooks._default.set( this );
			}
			return this;
		}
	};

	Tween.prototype.init.prototype = Tween.prototype;

	Tween.propHooks = {
		_default: {
			get: function( tween ) {
				var result;

				// Use a property on the element directly when it is not a DOM element,
				// or when there is no matching style property that exists.
				if ( tween.elem.nodeType !== 1 ||
					tween.elem[ tween.prop ] != null && tween.elem.style[ tween.prop ] == null ) {
					return tween.elem[ tween.prop ];
				}

				// Passing an empty string as a 3rd parameter to .css will automatically
				// attempt a parseFloat and fallback to a string if the parse fails.
				// Simple values such as "10px" are parsed to Float;
				// complex values such as "rotate(1rad)" are returned as-is.
				result = jQuery.css( tween.elem, tween.prop, "" );

				// Empty strings, null, undefined and "auto" are converted to 0.
				return !result || result === "auto" ? 0 : result;
			},
			set: function( tween ) {

				// Use step hook for back compat.
				// Use cssHook if its there.
				// Use .style if available and use plain properties where available.
				if ( jQuery.fx.step[ tween.prop ] ) {
					jQuery.fx.step[ tween.prop ]( tween );
				} else if ( tween.elem.nodeType === 1 &&
					( tween.elem.style[ jQuery.cssProps[ tween.prop ] ] != null ||
						jQuery.cssHooks[ tween.prop ] ) ) {
					jQuery.style( tween.elem, tween.prop, tween.now + tween.unit );
				} else {
					tween.elem[ tween.prop ] = tween.now;
				}
			}
		}
	};

	// Support: IE <=9 only
	// Panic based approach to setting things on disconnected nodes
	Tween.propHooks.scrollTop = Tween.propHooks.scrollLeft = {
		set: function( tween ) {
			if ( tween.elem.nodeType && tween.elem.parentNode ) {
				tween.elem[ tween.prop ] = tween.now;
			}
		}
	};

	jQuery.easing = {
		linear: function( p ) {
			return p;
		},
		swing: function( p ) {
			return 0.5 - Math.cos( p * Math.PI ) / 2;
		},
		_default: "swing"
	};

	jQuery.fx = Tween.prototype.init;

	// Back compat <1.8 extension point
	jQuery.fx.step = {};




	var
		fxNow, timerId,
		rfxtypes = /^(?:toggle|show|hide)$/,
		rrun = /queueHooks$/;

	function raf() {
		if ( timerId ) {
			window.requestAnimationFrame( raf );
			jQuery.fx.tick();
		}
	}

	// Animations created synchronously will run synchronously
	function createFxNow() {
		window.setTimeout( function() {
			fxNow = undefined;
		} );
		return ( fxNow = jQuery.now() );
	}

	// Generate parameters to create a standard animation
	function genFx( type, includeWidth ) {
		var which,
			i = 0,
			attrs = { height: type };

		// If we include width, step value is 1 to do all cssExpand values,
		// otherwise step value is 2 to skip over Left and Right
		includeWidth = includeWidth ? 1 : 0;
		for ( ; i < 4; i += 2 - includeWidth ) {
			which = cssExpand[ i ];
			attrs[ "margin" + which ] = attrs[ "padding" + which ] = type;
		}

		if ( includeWidth ) {
			attrs.opacity = attrs.width = type;
		}

		return attrs;
	}

	function createTween( value, prop, animation ) {
		var tween,
			collection = ( Animation.tweeners[ prop ] || [] ).concat( Animation.tweeners[ "*" ] ),
			index = 0,
			length = collection.length;
		for ( ; index < length; index++ ) {
			if ( ( tween = collection[ index ].call( animation, prop, value ) ) ) {

				// We're done with this property
				return tween;
			}
		}
	}

	function defaultPrefilter( elem, props, opts ) {
		var prop, value, toggle, hooks, oldfire, propTween, restoreDisplay, display,
			isBox = "width" in props || "height" in props,
			anim = this,
			orig = {},
			style = elem.style,
			hidden = elem.nodeType && isHiddenWithinTree( elem ),
			dataShow = dataPriv.get( elem, "fxshow" );

		// Queue-skipping animations hijack the fx hooks
		if ( !opts.queue ) {
			hooks = jQuery._queueHooks( elem, "fx" );
			if ( hooks.unqueued == null ) {
				hooks.unqueued = 0;
				oldfire = hooks.empty.fire;
				hooks.empty.fire = function() {
					if ( !hooks.unqueued ) {
						oldfire();
					}
				};
			}
			hooks.unqueued++;

			anim.always( function() {

				// Ensure the complete handler is called before this completes
				anim.always( function() {
					hooks.unqueued--;
					if ( !jQuery.queue( elem, "fx" ).length ) {
						hooks.empty.fire();
					}
				} );
			} );
		}

		// Detect show/hide animations
		for ( prop in props ) {
			value = props[ prop ];
			if ( rfxtypes.test( value ) ) {
				delete props[ prop ];
				toggle = toggle || value === "toggle";
				if ( value === ( hidden ? "hide" : "show" ) ) {

					// Pretend to be hidden if this is a "show" and
					// there is still data from a stopped show/hide
					if ( value === "show" && dataShow && dataShow[ prop ] !== undefined ) {
						hidden = true;

					// Ignore all other no-op show/hide data
					} else {
						continue;
					}
				}
				orig[ prop ] = dataShow && dataShow[ prop ] || jQuery.style( elem, prop );
			}
		}

		// Bail out if this is a no-op like .hide().hide()
		propTween = !jQuery.isEmptyObject( props );
		if ( !propTween && jQuery.isEmptyObject( orig ) ) {
			return;
		}

		// Restrict "overflow" and "display" styles during box animations
		if ( isBox && elem.nodeType === 1 ) {

			// Support: IE <=9 - 11, Edge 12 - 13
			// Record all 3 overflow attributes because IE does not infer the shorthand
			// from identically-valued overflowX and overflowY
			opts.overflow = [ style.overflow, style.overflowX, style.overflowY ];

			// Identify a display type, preferring old show/hide data over the CSS cascade
			restoreDisplay = dataShow && dataShow.display;
			if ( restoreDisplay == null ) {
				restoreDisplay = dataPriv.get( elem, "display" );
			}
			display = jQuery.css( elem, "display" );
			if ( display === "none" ) {
				if ( restoreDisplay ) {
					display = restoreDisplay;
				} else {

					// Get nonempty value(s) by temporarily forcing visibility
					showHide( [ elem ], true );
					restoreDisplay = elem.style.display || restoreDisplay;
					display = jQuery.css( elem, "display" );
					showHide( [ elem ] );
				}
			}

			// Animate inline elements as inline-block
			if ( display === "inline" || display === "inline-block" && restoreDisplay != null ) {
				if ( jQuery.css( elem, "float" ) === "none" ) {

					// Restore the original display value at the end of pure show/hide animations
					if ( !propTween ) {
						anim.done( function() {
							style.display = restoreDisplay;
						} );
						if ( restoreDisplay == null ) {
							display = style.display;
							restoreDisplay = display === "none" ? "" : display;
						}
					}
					style.display = "inline-block";
				}
			}
		}

		if ( opts.overflow ) {
			style.overflow = "hidden";
			anim.always( function() {
				style.overflow = opts.overflow[ 0 ];
				style.overflowX = opts.overflow[ 1 ];
				style.overflowY = opts.overflow[ 2 ];
			} );
		}

		// Implement show/hide animations
		propTween = false;
		for ( prop in orig ) {

			// General show/hide setup for this element animation
			if ( !propTween ) {
				if ( dataShow ) {
					if ( "hidden" in dataShow ) {
						hidden = dataShow.hidden;
					}
				} else {
					dataShow = dataPriv.access( elem, "fxshow", { display: restoreDisplay } );
				}

				// Store hidden/visible for toggle so `.stop().toggle()` "reverses"
				if ( toggle ) {
					dataShow.hidden = !hidden;
				}

				// Show elements before animating them
				if ( hidden ) {
					showHide( [ elem ], true );
				}

				/* eslint-disable no-loop-func */

				anim.done( function() {

				/* eslint-enable no-loop-func */

					// The final step of a "hide" animation is actually hiding the element
					if ( !hidden ) {
						showHide( [ elem ] );
					}
					dataPriv.remove( elem, "fxshow" );
					for ( prop in orig ) {
						jQuery.style( elem, prop, orig[ prop ] );
					}
				} );
			}

			// Per-property setup
			propTween = createTween( hidden ? dataShow[ prop ] : 0, prop, anim );
			if ( !( prop in dataShow ) ) {
				dataShow[ prop ] = propTween.start;
				if ( hidden ) {
					propTween.end = propTween.start;
					propTween.start = 0;
				}
			}
		}
	}

	function propFilter( props, specialEasing ) {
		var index, name, easing, value, hooks;

		// camelCase, specialEasing and expand cssHook pass
		for ( index in props ) {
			name = jQuery.camelCase( index );
			easing = specialEasing[ name ];
			value = props[ index ];
			if ( jQuery.isArray( value ) ) {
				easing = value[ 1 ];
				value = props[ index ] = value[ 0 ];
			}

			if ( index !== name ) {
				props[ name ] = value;
				delete props[ index ];
			}

			hooks = jQuery.cssHooks[ name ];
			if ( hooks && "expand" in hooks ) {
				value = hooks.expand( value );
				delete props[ name ];

				// Not quite $.extend, this won't overwrite existing keys.
				// Reusing 'index' because we have the correct "name"
				for ( index in value ) {
					if ( !( index in props ) ) {
						props[ index ] = value[ index ];
						specialEasing[ index ] = easing;
					}
				}
			} else {
				specialEasing[ name ] = easing;
			}
		}
	}

	function Animation( elem, properties, options ) {
		var result,
			stopped,
			index = 0,
			length = Animation.prefilters.length,
			deferred = jQuery.Deferred().always( function() {

				// Don't match elem in the :animated selector
				delete tick.elem;
			} ),
			tick = function() {
				if ( stopped ) {
					return false;
				}
				var currentTime = fxNow || createFxNow(),
					remaining = Math.max( 0, animation.startTime + animation.duration - currentTime ),

					// Support: Android 2.3 only
					// Archaic crash bug won't allow us to use `1 - ( 0.5 || 0 )` (#12497)
					temp = remaining / animation.duration || 0,
					percent = 1 - temp,
					index = 0,
					length = animation.tweens.length;

				for ( ; index < length; index++ ) {
					animation.tweens[ index ].run( percent );
				}

				deferred.notifyWith( elem, [ animation, percent, remaining ] );

				if ( percent < 1 && length ) {
					return remaining;
				} else {
					deferred.resolveWith( elem, [ animation ] );
					return false;
				}
			},
			animation = deferred.promise( {
				elem: elem,
				props: jQuery.extend( {}, properties ),
				opts: jQuery.extend( true, {
					specialEasing: {},
					easing: jQuery.easing._default
				}, options ),
				originalProperties: properties,
				originalOptions: options,
				startTime: fxNow || createFxNow(),
				duration: options.duration,
				tweens: [],
				createTween: function( prop, end ) {
					var tween = jQuery.Tween( elem, animation.opts, prop, end,
							animation.opts.specialEasing[ prop ] || animation.opts.easing );
					animation.tweens.push( tween );
					return tween;
				},
				stop: function( gotoEnd ) {
					var index = 0,

						// If we are going to the end, we want to run all the tweens
						// otherwise we skip this part
						length = gotoEnd ? animation.tweens.length : 0;
					if ( stopped ) {
						return this;
					}
					stopped = true;
					for ( ; index < length; index++ ) {
						animation.tweens[ index ].run( 1 );
					}

					// Resolve when we played the last frame; otherwise, reject
					if ( gotoEnd ) {
						deferred.notifyWith( elem, [ animation, 1, 0 ] );
						deferred.resolveWith( elem, [ animation, gotoEnd ] );
					} else {
						deferred.rejectWith( elem, [ animation, gotoEnd ] );
					}
					return this;
				}
			} ),
			props = animation.props;

		propFilter( props, animation.opts.specialEasing );

		for ( ; index < length; index++ ) {
			result = Animation.prefilters[ index ].call( animation, elem, props, animation.opts );
			if ( result ) {
				if ( jQuery.isFunction( result.stop ) ) {
					jQuery._queueHooks( animation.elem, animation.opts.queue ).stop =
						jQuery.proxy( result.stop, result );
				}
				return result;
			}
		}

		jQuery.map( props, createTween, animation );

		if ( jQuery.isFunction( animation.opts.start ) ) {
			animation.opts.start.call( elem, animation );
		}

		jQuery.fx.timer(
			jQuery.extend( tick, {
				elem: elem,
				anim: animation,
				queue: animation.opts.queue
			} )
		);

		// attach callbacks from options
		return animation.progress( animation.opts.progress )
			.done( animation.opts.done, animation.opts.complete )
			.fail( animation.opts.fail )
			.always( animation.opts.always );
	}

	jQuery.Animation = jQuery.extend( Animation, {

		tweeners: {
			"*": [ function( prop, value ) {
				var tween = this.createTween( prop, value );
				adjustCSS( tween.elem, prop, rcssNum.exec( value ), tween );
				return tween;
			} ]
		},

		tweener: function( props, callback ) {
			if ( jQuery.isFunction( props ) ) {
				callback = props;
				props = [ "*" ];
			} else {
				props = props.match( rnotwhite );
			}

			var prop,
				index = 0,
				length = props.length;

			for ( ; index < length; index++ ) {
				prop = props[ index ];
				Animation.tweeners[ prop ] = Animation.tweeners[ prop ] || [];
				Animation.tweeners[ prop ].unshift( callback );
			}
		},

		prefilters: [ defaultPrefilter ],

		prefilter: function( callback, prepend ) {
			if ( prepend ) {
				Animation.prefilters.unshift( callback );
			} else {
				Animation.prefilters.push( callback );
			}
		}
	} );

	jQuery.speed = function( speed, easing, fn ) {
		var opt = speed && typeof speed === "object" ? jQuery.extend( {}, speed ) : {
			complete: fn || !fn && easing ||
				jQuery.isFunction( speed ) && speed,
			duration: speed,
			easing: fn && easing || easing && !jQuery.isFunction( easing ) && easing
		};

		// Go to the end state if fx are off or if document is hidden
		if ( jQuery.fx.off || document.hidden ) {
			opt.duration = 0;

		} else {
			opt.duration = typeof opt.duration === "number" ?
				opt.duration : opt.duration in jQuery.fx.speeds ?
					jQuery.fx.speeds[ opt.duration ] : jQuery.fx.speeds._default;
		}

		// Normalize opt.queue - true/undefined/null -> "fx"
		if ( opt.queue == null || opt.queue === true ) {
			opt.queue = "fx";
		}

		// Queueing
		opt.old = opt.complete;

		opt.complete = function() {
			if ( jQuery.isFunction( opt.old ) ) {
				opt.old.call( this );
			}

			if ( opt.queue ) {
				jQuery.dequeue( this, opt.queue );
			}
		};

		return opt;
	};

	jQuery.fn.extend( {
		fadeTo: function( speed, to, easing, callback ) {

			// Show any hidden elements after setting opacity to 0
			return this.filter( isHiddenWithinTree ).css( "opacity", 0 ).show()

				// Animate to the value specified
				.end().animate( { opacity: to }, speed, easing, callback );
		},
		animate: function( prop, speed, easing, callback ) {
			var empty = jQuery.isEmptyObject( prop ),
				optall = jQuery.speed( speed, easing, callback ),
				doAnimation = function() {

					// Operate on a copy of prop so per-property easing won't be lost
					var anim = Animation( this, jQuery.extend( {}, prop ), optall );

					// Empty animations, or finishing resolves immediately
					if ( empty || dataPriv.get( this, "finish" ) ) {
						anim.stop( true );
					}
				};
				doAnimation.finish = doAnimation;

			return empty || optall.queue === false ?
				this.each( doAnimation ) :
				this.queue( optall.queue, doAnimation );
		},
		stop: function( type, clearQueue, gotoEnd ) {
			var stopQueue = function( hooks ) {
				var stop = hooks.stop;
				delete hooks.stop;
				stop( gotoEnd );
			};

			if ( typeof type !== "string" ) {
				gotoEnd = clearQueue;
				clearQueue = type;
				type = undefined;
			}
			if ( clearQueue && type !== false ) {
				this.queue( type || "fx", [] );
			}

			return this.each( function() {
				var dequeue = true,
					index = type != null && type + "queueHooks",
					timers = jQuery.timers,
					data = dataPriv.get( this );

				if ( index ) {
					if ( data[ index ] && data[ index ].stop ) {
						stopQueue( data[ index ] );
					}
				} else {
					for ( index in data ) {
						if ( data[ index ] && data[ index ].stop && rrun.test( index ) ) {
							stopQueue( data[ index ] );
						}
					}
				}

				for ( index = timers.length; index--; ) {
					if ( timers[ index ].elem === this &&
						( type == null || timers[ index ].queue === type ) ) {

						timers[ index ].anim.stop( gotoEnd );
						dequeue = false;
						timers.splice( index, 1 );
					}
				}

				// Start the next in the queue if the last step wasn't forced.
				// Timers currently will call their complete callbacks, which
				// will dequeue but only if they were gotoEnd.
				if ( dequeue || !gotoEnd ) {
					jQuery.dequeue( this, type );
				}
			} );
		},
		finish: function( type ) {
			if ( type !== false ) {
				type = type || "fx";
			}
			return this.each( function() {
				var index,
					data = dataPriv.get( this ),
					queue = data[ type + "queue" ],
					hooks = data[ type + "queueHooks" ],
					timers = jQuery.timers,
					length = queue ? queue.length : 0;

				// Enable finishing flag on private data
				data.finish = true;

				// Empty the queue first
				jQuery.queue( this, type, [] );

				if ( hooks && hooks.stop ) {
					hooks.stop.call( this, true );
				}

				// Look for any active animations, and finish them
				for ( index = timers.length; index--; ) {
					if ( timers[ index ].elem === this && timers[ index ].queue === type ) {
						timers[ index ].anim.stop( true );
						timers.splice( index, 1 );
					}
				}

				// Look for any animations in the old queue and finish them
				for ( index = 0; index < length; index++ ) {
					if ( queue[ index ] && queue[ index ].finish ) {
						queue[ index ].finish.call( this );
					}
				}

				// Turn off finishing flag
				delete data.finish;
			} );
		}
	} );

	jQuery.each( [ "toggle", "show", "hide" ], function( i, name ) {
		var cssFn = jQuery.fn[ name ];
		jQuery.fn[ name ] = function( speed, easing, callback ) {
			return speed == null || typeof speed === "boolean" ?
				cssFn.apply( this, arguments ) :
				this.animate( genFx( name, true ), speed, easing, callback );
		};
	} );

	// Generate shortcuts for custom animations
	jQuery.each( {
		slideDown: genFx( "show" ),
		slideUp: genFx( "hide" ),
		slideToggle: genFx( "toggle" ),
		fadeIn: { opacity: "show" },
		fadeOut: { opacity: "hide" },
		fadeToggle: { opacity: "toggle" }
	}, function( name, props ) {
		jQuery.fn[ name ] = function( speed, easing, callback ) {
			return this.animate( props, speed, easing, callback );
		};
	} );

	jQuery.timers = [];
	jQuery.fx.tick = function() {
		var timer,
			i = 0,
			timers = jQuery.timers;

		fxNow = jQuery.now();

		for ( ; i < timers.length; i++ ) {
			timer = timers[ i ];

			// Checks the timer has not already been removed
			if ( !timer() && timers[ i ] === timer ) {
				timers.splice( i--, 1 );
			}
		}

		if ( !timers.length ) {
			jQuery.fx.stop();
		}
		fxNow = undefined;
	};

	jQuery.fx.timer = function( timer ) {
		jQuery.timers.push( timer );
		if ( timer() ) {
			jQuery.fx.start();
		} else {
			jQuery.timers.pop();
		}
	};

	jQuery.fx.interval = 13;
	jQuery.fx.start = function() {
		if ( !timerId ) {
			timerId = window.requestAnimationFrame ?
				window.requestAnimationFrame( raf ) :
				window.setInterval( jQuery.fx.tick, jQuery.fx.interval );
		}
	};

	jQuery.fx.stop = function() {
		if ( window.cancelAnimationFrame ) {
			window.cancelAnimationFrame( timerId );
		} else {
			window.clearInterval( timerId );
		}

		timerId = null;
	};

	jQuery.fx.speeds = {
		slow: 600,
		fast: 200,

		// Default speed
		_default: 400
	};


	// Based off of the plugin by Clint Helfers, with permission.
	// https://web.archive.org/web/20100324014747/http://blindsignals.com/index.php/2009/07/jquery-delay/
	jQuery.fn.delay = function( time, type ) {
		time = jQuery.fx ? jQuery.fx.speeds[ time ] || time : time;
		type = type || "fx";

		return this.queue( type, function( next, hooks ) {
			var timeout = window.setTimeout( next, time );
			hooks.stop = function() {
				window.clearTimeout( timeout );
			};
		} );
	};


	( function() {
		var input = document.createElement( "input" ),
			select = document.createElement( "select" ),
			opt = select.appendChild( document.createElement( "option" ) );

		input.type = "checkbox";

		// Support: Android <=4.3 only
		// Default value for a checkbox should be "on"
		support.checkOn = input.value !== "";

		// Support: IE <=11 only
		// Must access selectedIndex to make default options select
		support.optSelected = opt.selected;

		// Support: IE <=11 only
		// An input loses its value after becoming a radio
		input = document.createElement( "input" );
		input.value = "t";
		input.type = "radio";
		support.radioValue = input.value === "t";
	} )();


	var boolHook,
		attrHandle = jQuery.expr.attrHandle;

	jQuery.fn.extend( {
		attr: function( name, value ) {
			return access( this, jQuery.attr, name, value, arguments.length > 1 );
		},

		removeAttr: function( name ) {
			return this.each( function() {
				jQuery.removeAttr( this, name );
			} );
		}
	} );

	jQuery.extend( {
		attr: function( elem, name, value ) {
			var ret, hooks,
				nType = elem.nodeType;

			// Don't get/set attributes on text, comment and attribute nodes
			if ( nType === 3 || nType === 8 || nType === 2 ) {
				return;
			}

			// Fallback to prop when attributes are not supported
			if ( typeof elem.getAttribute === "undefined" ) {
				return jQuery.prop( elem, name, value );
			}

			// Attribute hooks are determined by the lowercase version
			// Grab necessary hook if one is defined
			if ( nType !== 1 || !jQuery.isXMLDoc( elem ) ) {
				hooks = jQuery.attrHooks[ name.toLowerCase() ] ||
					( jQuery.expr.match.bool.test( name ) ? boolHook : undefined );
			}

			if ( value !== undefined ) {
				if ( value === null ) {
					jQuery.removeAttr( elem, name );
					return;
				}

				if ( hooks && "set" in hooks &&
					( ret = hooks.set( elem, value, name ) ) !== undefined ) {
					return ret;
				}

				elem.setAttribute( name, value + "" );
				return value;
			}

			if ( hooks && "get" in hooks && ( ret = hooks.get( elem, name ) ) !== null ) {
				return ret;
			}

			ret = jQuery.find.attr( elem, name );

			// Non-existent attributes return null, we normalize to undefined
			return ret == null ? undefined : ret;
		},

		attrHooks: {
			type: {
				set: function( elem, value ) {
					if ( !support.radioValue && value === "radio" &&
						jQuery.nodeName( elem, "input" ) ) {
						var val = elem.value;
						elem.setAttribute( "type", value );
						if ( val ) {
							elem.value = val;
						}
						return value;
					}
				}
			}
		},

		removeAttr: function( elem, value ) {
			var name,
				i = 0,
				attrNames = value && value.match( rnotwhite );

			if ( attrNames && elem.nodeType === 1 ) {
				while ( ( name = attrNames[ i++ ] ) ) {
					elem.removeAttribute( name );
				}
			}
		}
	} );

	// Hooks for boolean attributes
	boolHook = {
		set: function( elem, value, name ) {
			if ( value === false ) {

				// Remove boolean attributes when set to false
				jQuery.removeAttr( elem, name );
			} else {
				elem.setAttribute( name, name );
			}
			return name;
		}
	};

	jQuery.each( jQuery.expr.match.bool.source.match( /\w+/g ), function( i, name ) {
		var getter = attrHandle[ name ] || jQuery.find.attr;

		attrHandle[ name ] = function( elem, name, isXML ) {
			var ret, handle,
				lowercaseName = name.toLowerCase();

			if ( !isXML ) {

				// Avoid an infinite loop by temporarily removing this function from the getter
				handle = attrHandle[ lowercaseName ];
				attrHandle[ lowercaseName ] = ret;
				ret = getter( elem, name, isXML ) != null ?
					lowercaseName :
					null;
				attrHandle[ lowercaseName ] = handle;
			}
			return ret;
		};
	} );




	var rfocusable = /^(?:input|select|textarea|button)$/i,
		rclickable = /^(?:a|area)$/i;

	jQuery.fn.extend( {
		prop: function( name, value ) {
			return access( this, jQuery.prop, name, value, arguments.length > 1 );
		},

		removeProp: function( name ) {
			return this.each( function() {
				delete this[ jQuery.propFix[ name ] || name ];
			} );
		}
	} );

	jQuery.extend( {
		prop: function( elem, name, value ) {
			var ret, hooks,
				nType = elem.nodeType;

			// Don't get/set properties on text, comment and attribute nodes
			if ( nType === 3 || nType === 8 || nType === 2 ) {
				return;
			}

			if ( nType !== 1 || !jQuery.isXMLDoc( elem ) ) {

				// Fix name and attach hooks
				name = jQuery.propFix[ name ] || name;
				hooks = jQuery.propHooks[ name ];
			}

			if ( value !== undefined ) {
				if ( hooks && "set" in hooks &&
					( ret = hooks.set( elem, value, name ) ) !== undefined ) {
					return ret;
				}

				return ( elem[ name ] = value );
			}

			if ( hooks && "get" in hooks && ( ret = hooks.get( elem, name ) ) !== null ) {
				return ret;
			}

			return elem[ name ];
		},

		propHooks: {
			tabIndex: {
				get: function( elem ) {

					// Support: IE <=9 - 11 only
					// elem.tabIndex doesn't always return the
					// correct value when it hasn't been explicitly set
					// https://web.archive.org/web/20141116233347/http://fluidproject.org/blog/2008/01/09/getting-setting-and-removing-tabindex-values-with-javascript/
					// Use proper attribute retrieval(#12072)
					var tabindex = jQuery.find.attr( elem, "tabindex" );

					return tabindex ?
						parseInt( tabindex, 10 ) :
						rfocusable.test( elem.nodeName ) ||
							rclickable.test( elem.nodeName ) && elem.href ?
								0 :
								-1;
				}
			}
		},

		propFix: {
			"for": "htmlFor",
			"class": "className"
		}
	} );

	// Support: IE <=11 only
	// Accessing the selectedIndex property
	// forces the browser to respect setting selected
	// on the option
	// The getter ensures a default option is selected
	// when in an optgroup
	if ( !support.optSelected ) {
		jQuery.propHooks.selected = {
			get: function( elem ) {
				var parent = elem.parentNode;
				if ( parent && parent.parentNode ) {
					parent.parentNode.selectedIndex;
				}
				return null;
			},
			set: function( elem ) {
				var parent = elem.parentNode;
				if ( parent ) {
					parent.selectedIndex;

					if ( parent.parentNode ) {
						parent.parentNode.selectedIndex;
					}
				}
			}
		};
	}

	jQuery.each( [
		"tabIndex",
		"readOnly",
		"maxLength",
		"cellSpacing",
		"cellPadding",
		"rowSpan",
		"colSpan",
		"useMap",
		"frameBorder",
		"contentEditable"
	], function() {
		jQuery.propFix[ this.toLowerCase() ] = this;
	} );




	var rclass = /[\t\r\n\f]/g;

	function getClass( elem ) {
		return elem.getAttribute && elem.getAttribute( "class" ) || "";
	}

	jQuery.fn.extend( {
		addClass: function( value ) {
			var classes, elem, cur, curValue, clazz, j, finalValue,
				i = 0;

			if ( jQuery.isFunction( value ) ) {
				return this.each( function( j ) {
					jQuery( this ).addClass( value.call( this, j, getClass( this ) ) );
				} );
			}

			if ( typeof value === "string" && value ) {
				classes = value.match( rnotwhite ) || [];

				while ( ( elem = this[ i++ ] ) ) {
					curValue = getClass( elem );
					cur = elem.nodeType === 1 &&
						( " " + curValue + " " ).replace( rclass, " " );

					if ( cur ) {
						j = 0;
						while ( ( clazz = classes[ j++ ] ) ) {
							if ( cur.indexOf( " " + clazz + " " ) < 0 ) {
								cur += clazz + " ";
							}
						}

						// Only assign if different to avoid unneeded rendering.
						finalValue = jQuery.trim( cur );
						if ( curValue !== finalValue ) {
							elem.setAttribute( "class", finalValue );
						}
					}
				}
			}

			return this;
		},

		removeClass: function( value ) {
			var classes, elem, cur, curValue, clazz, j, finalValue,
				i = 0;

			if ( jQuery.isFunction( value ) ) {
				return this.each( function( j ) {
					jQuery( this ).removeClass( value.call( this, j, getClass( this ) ) );
				} );
			}

			if ( !arguments.length ) {
				return this.attr( "class", "" );
			}

			if ( typeof value === "string" && value ) {
				classes = value.match( rnotwhite ) || [];

				while ( ( elem = this[ i++ ] ) ) {
					curValue = getClass( elem );

					// This expression is here for better compressibility (see addClass)
					cur = elem.nodeType === 1 &&
						( " " + curValue + " " ).replace( rclass, " " );

					if ( cur ) {
						j = 0;
						while ( ( clazz = classes[ j++ ] ) ) {

							// Remove *all* instances
							while ( cur.indexOf( " " + clazz + " " ) > -1 ) {
								cur = cur.replace( " " + clazz + " ", " " );
							}
						}

						// Only assign if different to avoid unneeded rendering.
						finalValue = jQuery.trim( cur );
						if ( curValue !== finalValue ) {
							elem.setAttribute( "class", finalValue );
						}
					}
				}
			}

			return this;
		},

		toggleClass: function( value, stateVal ) {
			var type = typeof value;

			if ( typeof stateVal === "boolean" && type === "string" ) {
				return stateVal ? this.addClass( value ) : this.removeClass( value );
			}

			if ( jQuery.isFunction( value ) ) {
				return this.each( function( i ) {
					jQuery( this ).toggleClass(
						value.call( this, i, getClass( this ), stateVal ),
						stateVal
					);
				} );
			}

			return this.each( function() {
				var className, i, self, classNames;

				if ( type === "string" ) {

					// Toggle individual class names
					i = 0;
					self = jQuery( this );
					classNames = value.match( rnotwhite ) || [];

					while ( ( className = classNames[ i++ ] ) ) {

						// Check each className given, space separated list
						if ( self.hasClass( className ) ) {
							self.removeClass( className );
						} else {
							self.addClass( className );
						}
					}

				// Toggle whole class name
				} else if ( value === undefined || type === "boolean" ) {
					className = getClass( this );
					if ( className ) {

						// Store className if set
						dataPriv.set( this, "__className__", className );
					}

					// If the element has a class name or if we're passed `false`,
					// then remove the whole classname (if there was one, the above saved it).
					// Otherwise bring back whatever was previously saved (if anything),
					// falling back to the empty string if nothing was stored.
					if ( this.setAttribute ) {
						this.setAttribute( "class",
							className || value === false ?
							"" :
							dataPriv.get( this, "__className__" ) || ""
						);
					}
				}
			} );
		},

		hasClass: function( selector ) {
			var className, elem,
				i = 0;

			className = " " + selector + " ";
			while ( ( elem = this[ i++ ] ) ) {
				if ( elem.nodeType === 1 &&
					( " " + getClass( elem ) + " " ).replace( rclass, " " )
						.indexOf( className ) > -1
				) {
					return true;
				}
			}

			return false;
		}
	} );




	var rreturn = /\r/g,
		rspaces = /[\x20\t\r\n\f]+/g;

	jQuery.fn.extend( {
		val: function( value ) {
			var hooks, ret, isFunction,
				elem = this[ 0 ];

			if ( !arguments.length ) {
				if ( elem ) {
					hooks = jQuery.valHooks[ elem.type ] ||
						jQuery.valHooks[ elem.nodeName.toLowerCase() ];

					if ( hooks &&
						"get" in hooks &&
						( ret = hooks.get( elem, "value" ) ) !== undefined
					) {
						return ret;
					}

					ret = elem.value;

					return typeof ret === "string" ?

						// Handle most common string cases
						ret.replace( rreturn, "" ) :

						// Handle cases where value is null/undef or number
						ret == null ? "" : ret;
				}

				return;
			}

			isFunction = jQuery.isFunction( value );

			return this.each( function( i ) {
				var val;

				if ( this.nodeType !== 1 ) {
					return;
				}

				if ( isFunction ) {
					val = value.call( this, i, jQuery( this ).val() );
				} else {
					val = value;
				}

				// Treat null/undefined as ""; convert numbers to string
				if ( val == null ) {
					val = "";

				} else if ( typeof val === "number" ) {
					val += "";

				} else if ( jQuery.isArray( val ) ) {
					val = jQuery.map( val, function( value ) {
						return value == null ? "" : value + "";
					} );
				}

				hooks = jQuery.valHooks[ this.type ] || jQuery.valHooks[ this.nodeName.toLowerCase() ];

				// If set returns undefined, fall back to normal setting
				if ( !hooks || !( "set" in hooks ) || hooks.set( this, val, "value" ) === undefined ) {
					this.value = val;
				}
			} );
		}
	} );

	jQuery.extend( {
		valHooks: {
			option: {
				get: function( elem ) {

					var val = jQuery.find.attr( elem, "value" );
					return val != null ?
						val :

						// Support: IE <=10 - 11 only
						// option.text throws exceptions (#14686, #14858)
						// Strip and collapse whitespace
						// https://html.spec.whatwg.org/#strip-and-collapse-whitespace
						jQuery.trim( jQuery.text( elem ) ).replace( rspaces, " " );
				}
			},
			select: {
				get: function( elem ) {
					var value, option,
						options = elem.options,
						index = elem.selectedIndex,
						one = elem.type === "select-one",
						values = one ? null : [],
						max = one ? index + 1 : options.length,
						i = index < 0 ?
							max :
							one ? index : 0;

					// Loop through all the selected options
					for ( ; i < max; i++ ) {
						option = options[ i ];

						// Support: IE <=9 only
						// IE8-9 doesn't update selected after form reset (#2551)
						if ( ( option.selected || i === index ) &&

								// Don't return options that are disabled or in a disabled optgroup
								!option.disabled &&
								( !option.parentNode.disabled ||
									!jQuery.nodeName( option.parentNode, "optgroup" ) ) ) {

							// Get the specific value for the option
							value = jQuery( option ).val();

							// We don't need an array for one selects
							if ( one ) {
								return value;
							}

							// Multi-Selects return an array
							values.push( value );
						}
					}

					return values;
				},

				set: function( elem, value ) {
					var optionSet, option,
						options = elem.options,
						values = jQuery.makeArray( value ),
						i = options.length;

					while ( i-- ) {
						option = options[ i ];

						/* eslint-disable no-cond-assign */

						if ( option.selected =
							jQuery.inArray( jQuery.valHooks.option.get( option ), values ) > -1
						) {
							optionSet = true;
						}

						/* eslint-enable no-cond-assign */
					}

					// Force browsers to behave consistently when non-matching value is set
					if ( !optionSet ) {
						elem.selectedIndex = -1;
					}
					return values;
				}
			}
		}
	} );

	// Radios and checkboxes getter/setter
	jQuery.each( [ "radio", "checkbox" ], function() {
		jQuery.valHooks[ this ] = {
			set: function( elem, value ) {
				if ( jQuery.isArray( value ) ) {
					return ( elem.checked = jQuery.inArray( jQuery( elem ).val(), value ) > -1 );
				}
			}
		};
		if ( !support.checkOn ) {
			jQuery.valHooks[ this ].get = function( elem ) {
				return elem.getAttribute( "value" ) === null ? "on" : elem.value;
			};
		}
	} );




	// Return jQuery for attributes-only inclusion


	var rfocusMorph = /^(?:focusinfocus|focusoutblur)$/;

	jQuery.extend( jQuery.event, {

		trigger: function( event, data, elem, onlyHandlers ) {

			var i, cur, tmp, bubbleType, ontype, handle, special,
				eventPath = [ elem || document ],
				type = hasOwn.call( event, "type" ) ? event.type : event,
				namespaces = hasOwn.call( event, "namespace" ) ? event.namespace.split( "." ) : [];

			cur = tmp = elem = elem || document;

			// Don't do events on text and comment nodes
			if ( elem.nodeType === 3 || elem.nodeType === 8 ) {
				return;
			}

			// focus/blur morphs to focusin/out; ensure we're not firing them right now
			if ( rfocusMorph.test( type + jQuery.event.triggered ) ) {
				return;
			}

			if ( type.indexOf( "." ) > -1 ) {

				// Namespaced trigger; create a regexp to match event type in handle()
				namespaces = type.split( "." );
				type = namespaces.shift();
				namespaces.sort();
			}
			ontype = type.indexOf( ":" ) < 0 && "on" + type;

			// Caller can pass in a jQuery.Event object, Object, or just an event type string
			event = event[ jQuery.expando ] ?
				event :
				new jQuery.Event( type, typeof event === "object" && event );

			// Trigger bitmask: & 1 for native handlers; & 2 for jQuery (always true)
			event.isTrigger = onlyHandlers ? 2 : 3;
			event.namespace = namespaces.join( "." );
			event.rnamespace = event.namespace ?
				new RegExp( "(^|\\.)" + namespaces.join( "\\.(?:.*\\.|)" ) + "(\\.|$)" ) :
				null;

			// Clean up the event in case it is being reused
			event.result = undefined;
			if ( !event.target ) {
				event.target = elem;
			}

			// Clone any incoming data and prepend the event, creating the handler arg list
			data = data == null ?
				[ event ] :
				jQuery.makeArray( data, [ event ] );

			// Allow special events to draw outside the lines
			special = jQuery.event.special[ type ] || {};
			if ( !onlyHandlers && special.trigger && special.trigger.apply( elem, data ) === false ) {
				return;
			}

			// Determine event propagation path in advance, per W3C events spec (#9951)
			// Bubble up to document, then to window; watch for a global ownerDocument var (#9724)
			if ( !onlyHandlers && !special.noBubble && !jQuery.isWindow( elem ) ) {

				bubbleType = special.delegateType || type;
				if ( !rfocusMorph.test( bubbleType + type ) ) {
					cur = cur.parentNode;
				}
				for ( ; cur; cur = cur.parentNode ) {
					eventPath.push( cur );
					tmp = cur;
				}

				// Only add window if we got to document (e.g., not plain obj or detached DOM)
				if ( tmp === ( elem.ownerDocument || document ) ) {
					eventPath.push( tmp.defaultView || tmp.parentWindow || window );
				}
			}

			// Fire handlers on the event path
			i = 0;
			while ( ( cur = eventPath[ i++ ] ) && !event.isPropagationStopped() ) {

				event.type = i > 1 ?
					bubbleType :
					special.bindType || type;

				// jQuery handler
				handle = ( dataPriv.get( cur, "events" ) || {} )[ event.type ] &&
					dataPriv.get( cur, "handle" );
				if ( handle ) {
					handle.apply( cur, data );
				}

				// Native handler
				handle = ontype && cur[ ontype ];
				if ( handle && handle.apply && acceptData( cur ) ) {
					event.result = handle.apply( cur, data );
					if ( event.result === false ) {
						event.preventDefault();
					}
				}
			}
			event.type = type;

			// If nobody prevented the default action, do it now
			if ( !onlyHandlers && !event.isDefaultPrevented() ) {

				if ( ( !special._default ||
					special._default.apply( eventPath.pop(), data ) === false ) &&
					acceptData( elem ) ) {

					// Call a native DOM method on the target with the same name as the event.
					// Don't do default actions on window, that's where global variables be (#6170)
					if ( ontype && jQuery.isFunction( elem[ type ] ) && !jQuery.isWindow( elem ) ) {

						// Don't re-trigger an onFOO event when we call its FOO() method
						tmp = elem[ ontype ];

						if ( tmp ) {
							elem[ ontype ] = null;
						}

						// Prevent re-triggering of the same event, since we already bubbled it above
						jQuery.event.triggered = type;
						elem[ type ]();
						jQuery.event.triggered = undefined;

						if ( tmp ) {
							elem[ ontype ] = tmp;
						}
					}
				}
			}

			return event.result;
		},

		// Piggyback on a donor event to simulate a different one
		// Used only for `focus(in | out)` events
		simulate: function( type, elem, event ) {
			var e = jQuery.extend(
				new jQuery.Event(),
				event,
				{
					type: type,
					isSimulated: true
				}
			);

			jQuery.event.trigger( e, null, elem );
		}

	} );

	jQuery.fn.extend( {

		trigger: function( type, data ) {
			return this.each( function() {
				jQuery.event.trigger( type, data, this );
			} );
		},
		triggerHandler: function( type, data ) {
			var elem = this[ 0 ];
			if ( elem ) {
				return jQuery.event.trigger( type, data, elem, true );
			}
		}
	} );


	jQuery.each( ( "blur focus focusin focusout resize scroll click dblclick " +
		"mousedown mouseup mousemove mouseover mouseout mouseenter mouseleave " +
		"change select submit keydown keypress keyup contextmenu" ).split( " " ),
		function( i, name ) {

		// Handle event binding
		jQuery.fn[ name ] = function( data, fn ) {
			return arguments.length > 0 ?
				this.on( name, null, data, fn ) :
				this.trigger( name );
		};
	} );

	jQuery.fn.extend( {
		hover: function( fnOver, fnOut ) {
			return this.mouseenter( fnOver ).mouseleave( fnOut || fnOver );
		}
	} );




	support.focusin = "onfocusin" in window;


	// Support: Firefox <=44
	// Firefox doesn't have focus(in | out) events
	// Related ticket - https://bugzilla.mozilla.org/show_bug.cgi?id=687787
	//
	// Support: Chrome <=48 - 49, Safari <=9.0 - 9.1
	// focus(in | out) events fire after focus & blur events,
	// which is spec violation - http://www.w3.org/TR/DOM-Level-3-Events/#events-focusevent-event-order
	// Related ticket - https://bugs.chromium.org/p/chromium/issues/detail?id=449857
	if ( !support.focusin ) {
		jQuery.each( { focus: "focusin", blur: "focusout" }, function( orig, fix ) {

			// Attach a single capturing handler on the document while someone wants focusin/focusout
			var handler = function( event ) {
				jQuery.event.simulate( fix, event.target, jQuery.event.fix( event ) );
			};

			jQuery.event.special[ fix ] = {
				setup: function() {
					var doc = this.ownerDocument || this,
						attaches = dataPriv.access( doc, fix );

					if ( !attaches ) {
						doc.addEventListener( orig, handler, true );
					}
					dataPriv.access( doc, fix, ( attaches || 0 ) + 1 );
				},
				teardown: function() {
					var doc = this.ownerDocument || this,
						attaches = dataPriv.access( doc, fix ) - 1;

					if ( !attaches ) {
						doc.removeEventListener( orig, handler, true );
						dataPriv.remove( doc, fix );

					} else {
						dataPriv.access( doc, fix, attaches );
					}
				}
			};
		} );
	}
	var location = window.location;

	var nonce = jQuery.now();

	var rquery = ( /\?/ );



	// Cross-browser xml parsing
	jQuery.parseXML = function( data ) {
		var xml;
		if ( !data || typeof data !== "string" ) {
			return null;
		}

		// Support: IE 9 - 11 only
		// IE throws on parseFromString with invalid input.
		try {
			xml = ( new window.DOMParser() ).parseFromString( data, "text/xml" );
		} catch ( e ) {
			xml = undefined;
		}

		if ( !xml || xml.getElementsByTagName( "parsererror" ).length ) {
			jQuery.error( "Invalid XML: " + data );
		}
		return xml;
	};


	var
		rbracket = /\[\]$/,
		rCRLF = /\r?\n/g,
		rsubmitterTypes = /^(?:submit|button|image|reset|file)$/i,
		rsubmittable = /^(?:input|select|textarea|keygen)/i;

	function buildParams( prefix, obj, traditional, add ) {
		var name;

		if ( jQuery.isArray( obj ) ) {

			// Serialize array item.
			jQuery.each( obj, function( i, v ) {
				if ( traditional || rbracket.test( prefix ) ) {

					// Treat each array item as a scalar.
					add( prefix, v );

				} else {

					// Item is non-scalar (array or object), encode its numeric index.
					buildParams(
						prefix + "[" + ( typeof v === "object" && v != null ? i : "" ) + "]",
						v,
						traditional,
						add
					);
				}
			} );

		} else if ( !traditional && jQuery.type( obj ) === "object" ) {

			// Serialize object item.
			for ( name in obj ) {
				buildParams( prefix + "[" + name + "]", obj[ name ], traditional, add );
			}

		} else {

			// Serialize scalar item.
			add( prefix, obj );
		}
	}

	// Serialize an array of form elements or a set of
	// key/values into a query string
	jQuery.param = function( a, traditional ) {
		var prefix,
			s = [],
			add = function( key, valueOrFunction ) {

				// If value is a function, invoke it and use its return value
				var value = jQuery.isFunction( valueOrFunction ) ?
					valueOrFunction() :
					valueOrFunction;

				s[ s.length ] = encodeURIComponent( key ) + "=" +
					encodeURIComponent( value == null ? "" : value );
			};

		// If an array was passed in, assume that it is an array of form elements.
		if ( jQuery.isArray( a ) || ( a.jquery && !jQuery.isPlainObject( a ) ) ) {

			// Serialize the form elements
			jQuery.each( a, function() {
				add( this.name, this.value );
			} );

		} else {

			// If traditional, encode the "old" way (the way 1.3.2 or older
			// did it), otherwise encode params recursively.
			for ( prefix in a ) {
				buildParams( prefix, a[ prefix ], traditional, add );
			}
		}

		// Return the resulting serialization
		return s.join( "&" );
	};

	jQuery.fn.extend( {
		serialize: function() {
			return jQuery.param( this.serializeArray() );
		},
		serializeArray: function() {
			return this.map( function() {

				// Can add propHook for "elements" to filter or add form elements
				var elements = jQuery.prop( this, "elements" );
				return elements ? jQuery.makeArray( elements ) : this;
			} )
			.filter( function() {
				var type = this.type;

				// Use .is( ":disabled" ) so that fieldset[disabled] works
				return this.name && !jQuery( this ).is( ":disabled" ) &&
					rsubmittable.test( this.nodeName ) && !rsubmitterTypes.test( type ) &&
					( this.checked || !rcheckableType.test( type ) );
			} )
			.map( function( i, elem ) {
				var val = jQuery( this ).val();

				return val == null ?
					null :
					jQuery.isArray( val ) ?
						jQuery.map( val, function( val ) {
							return { name: elem.name, value: val.replace( rCRLF, "\r\n" ) };
						} ) :
						{ name: elem.name, value: val.replace( rCRLF, "\r\n" ) };
			} ).get();
		}
	} );


	var
		r20 = /%20/g,
		rhash = /#.*$/,
		rts = /([?&])_=[^&]*/,
		rheaders = /^(.*?):[ \t]*([^\r\n]*)$/mg,

		// #7653, #8125, #8152: local protocol detection
		rlocalProtocol = /^(?:about|app|app-storage|.+-extension|file|res|widget):$/,
		rnoContent = /^(?:GET|HEAD)$/,
		rprotocol = /^\/\//,

		/* Prefilters
		 * 1) They are useful to introduce custom dataTypes (see ajax/jsonp.js for an example)
		 * 2) These are called:
		 *    - BEFORE asking for a transport
		 *    - AFTER param serialization (s.data is a string if s.processData is true)
		 * 3) key is the dataType
		 * 4) the catchall symbol "*" can be used
		 * 5) execution will start with transport dataType and THEN continue down to "*" if needed
		 */
		prefilters = {},

		/* Transports bindings
		 * 1) key is the dataType
		 * 2) the catchall symbol "*" can be used
		 * 3) selection will start with transport dataType and THEN go to "*" if needed
		 */
		transports = {},

		// Avoid comment-prolog char sequence (#10098); must appease lint and evade compression
		allTypes = "*/".concat( "*" ),

		// Anchor tag for parsing the document origin
		originAnchor = document.createElement( "a" );
		originAnchor.href = location.href;

	// Base "constructor" for jQuery.ajaxPrefilter and jQuery.ajaxTransport
	function addToPrefiltersOrTransports( structure ) {

		// dataTypeExpression is optional and defaults to "*"
		return function( dataTypeExpression, func ) {

			if ( typeof dataTypeExpression !== "string" ) {
				func = dataTypeExpression;
				dataTypeExpression = "*";
			}

			var dataType,
				i = 0,
				dataTypes = dataTypeExpression.toLowerCase().match( rnotwhite ) || [];

			if ( jQuery.isFunction( func ) ) {

				// For each dataType in the dataTypeExpression
				while ( ( dataType = dataTypes[ i++ ] ) ) {

					// Prepend if requested
					if ( dataType[ 0 ] === "+" ) {
						dataType = dataType.slice( 1 ) || "*";
						( structure[ dataType ] = structure[ dataType ] || [] ).unshift( func );

					// Otherwise append
					} else {
						( structure[ dataType ] = structure[ dataType ] || [] ).push( func );
					}
				}
			}
		};
	}

	// Base inspection function for prefilters and transports
	function inspectPrefiltersOrTransports( structure, options, originalOptions, jqXHR ) {

		var inspected = {},
			seekingTransport = ( structure === transports );

		function inspect( dataType ) {
			var selected;
			inspected[ dataType ] = true;
			jQuery.each( structure[ dataType ] || [], function( _, prefilterOrFactory ) {
				var dataTypeOrTransport = prefilterOrFactory( options, originalOptions, jqXHR );
				if ( typeof dataTypeOrTransport === "string" &&
					!seekingTransport && !inspected[ dataTypeOrTransport ] ) {

					options.dataTypes.unshift( dataTypeOrTransport );
					inspect( dataTypeOrTransport );
					return false;
				} else if ( seekingTransport ) {
					return !( selected = dataTypeOrTransport );
				}
			} );
			return selected;
		}

		return inspect( options.dataTypes[ 0 ] ) || !inspected[ "*" ] && inspect( "*" );
	}

	// A special extend for ajax options
	// that takes "flat" options (not to be deep extended)
	// Fixes #9887
	function ajaxExtend( target, src ) {
		var key, deep,
			flatOptions = jQuery.ajaxSettings.flatOptions || {};

		for ( key in src ) {
			if ( src[ key ] !== undefined ) {
				( flatOptions[ key ] ? target : ( deep || ( deep = {} ) ) )[ key ] = src[ key ];
			}
		}
		if ( deep ) {
			jQuery.extend( true, target, deep );
		}

		return target;
	}

	/* Handles responses to an ajax request:
	 * - finds the right dataType (mediates between content-type and expected dataType)
	 * - returns the corresponding response
	 */
	function ajaxHandleResponses( s, jqXHR, responses ) {

		var ct, type, finalDataType, firstDataType,
			contents = s.contents,
			dataTypes = s.dataTypes;

		// Remove auto dataType and get content-type in the process
		while ( dataTypes[ 0 ] === "*" ) {
			dataTypes.shift();
			if ( ct === undefined ) {
				ct = s.mimeType || jqXHR.getResponseHeader( "Content-Type" );
			}
		}

		// Check if we're dealing with a known content-type
		if ( ct ) {
			for ( type in contents ) {
				if ( contents[ type ] && contents[ type ].test( ct ) ) {
					dataTypes.unshift( type );
					break;
				}
			}
		}

		// Check to see if we have a response for the expected dataType
		if ( dataTypes[ 0 ] in responses ) {
			finalDataType = dataTypes[ 0 ];
		} else {

			// Try convertible dataTypes
			for ( type in responses ) {
				if ( !dataTypes[ 0 ] || s.converters[ type + " " + dataTypes[ 0 ] ] ) {
					finalDataType = type;
					break;
				}
				if ( !firstDataType ) {
					firstDataType = type;
				}
			}

			// Or just use first one
			finalDataType = finalDataType || firstDataType;
		}

		// If we found a dataType
		// We add the dataType to the list if needed
		// and return the corresponding response
		if ( finalDataType ) {
			if ( finalDataType !== dataTypes[ 0 ] ) {
				dataTypes.unshift( finalDataType );
			}
			return responses[ finalDataType ];
		}
	}

	/* Chain conversions given the request and the original response
	 * Also sets the responseXXX fields on the jqXHR instance
	 */
	function ajaxConvert( s, response, jqXHR, isSuccess ) {
		var conv2, current, conv, tmp, prev,
			converters = {},

			// Work with a copy of dataTypes in case we need to modify it for conversion
			dataTypes = s.dataTypes.slice();

		// Create converters map with lowercased keys
		if ( dataTypes[ 1 ] ) {
			for ( conv in s.converters ) {
				converters[ conv.toLowerCase() ] = s.converters[ conv ];
			}
		}

		current = dataTypes.shift();

		// Convert to each sequential dataType
		while ( current ) {

			if ( s.responseFields[ current ] ) {
				jqXHR[ s.responseFields[ current ] ] = response;
			}

			// Apply the dataFilter if provided
			if ( !prev && isSuccess && s.dataFilter ) {
				response = s.dataFilter( response, s.dataType );
			}

			prev = current;
			current = dataTypes.shift();

			if ( current ) {

				// There's only work to do if current dataType is non-auto
				if ( current === "*" ) {

					current = prev;

				// Convert response if prev dataType is non-auto and differs from current
				} else if ( prev !== "*" && prev !== current ) {

					// Seek a direct converter
					conv = converters[ prev + " " + current ] || converters[ "* " + current ];

					// If none found, seek a pair
					if ( !conv ) {
						for ( conv2 in converters ) {

							// If conv2 outputs current
							tmp = conv2.split( " " );
							if ( tmp[ 1 ] === current ) {

								// If prev can be converted to accepted input
								conv = converters[ prev + " " + tmp[ 0 ] ] ||
									converters[ "* " + tmp[ 0 ] ];
								if ( conv ) {

									// Condense equivalence converters
									if ( conv === true ) {
										conv = converters[ conv2 ];

									// Otherwise, insert the intermediate dataType
									} else if ( converters[ conv2 ] !== true ) {
										current = tmp[ 0 ];
										dataTypes.unshift( tmp[ 1 ] );
									}
									break;
								}
							}
						}
					}

					// Apply converter (if not an equivalence)
					if ( conv !== true ) {

						// Unless errors are allowed to bubble, catch and return them
						if ( conv && s.throws ) {
							response = conv( response );
						} else {
							try {
								response = conv( response );
							} catch ( e ) {
								return {
									state: "parsererror",
									error: conv ? e : "No conversion from " + prev + " to " + current
								};
							}
						}
					}
				}
			}
		}

		return { state: "success", data: response };
	}

	jQuery.extend( {

		// Counter for holding the number of active queries
		active: 0,

		// Last-Modified header cache for next request
		lastModified: {},
		etag: {},

		ajaxSettings: {
			url: location.href,
			type: "GET",
			isLocal: rlocalProtocol.test( location.protocol ),
			global: true,
			processData: true,
			async: true,
			contentType: "application/x-www-form-urlencoded; charset=UTF-8",

			/*
			timeout: 0,
			data: null,
			dataType: null,
			username: null,
			password: null,
			cache: null,
			throws: false,
			traditional: false,
			headers: {},
			*/

			accepts: {
				"*": allTypes,
				text: "text/plain",
				html: "text/html",
				xml: "application/xml, text/xml",
				json: "application/json, text/javascript"
			},

			contents: {
				xml: /\bxml\b/,
				html: /\bhtml/,
				json: /\bjson\b/
			},

			responseFields: {
				xml: "responseXML",
				text: "responseText",
				json: "responseJSON"
			},

			// Data converters
			// Keys separate source (or catchall "*") and destination types with a single space
			converters: {

				// Convert anything to text
				"* text": String,

				// Text to html (true = no transformation)
				"text html": true,

				// Evaluate text as a json expression
				"text json": JSON.parse,

				// Parse text as xml
				"text xml": jQuery.parseXML
			},

			// For options that shouldn't be deep extended:
			// you can add your own custom options here if
			// and when you create one that shouldn't be
			// deep extended (see ajaxExtend)
			flatOptions: {
				url: true,
				context: true
			}
		},

		// Creates a full fledged settings object into target
		// with both ajaxSettings and settings fields.
		// If target is omitted, writes into ajaxSettings.
		ajaxSetup: function( target, settings ) {
			return settings ?

				// Building a settings object
				ajaxExtend( ajaxExtend( target, jQuery.ajaxSettings ), settings ) :

				// Extending ajaxSettings
				ajaxExtend( jQuery.ajaxSettings, target );
		},

		ajaxPrefilter: addToPrefiltersOrTransports( prefilters ),
		ajaxTransport: addToPrefiltersOrTransports( transports ),

		// Main method
		ajax: function( url, options ) {

			// If url is an object, simulate pre-1.5 signature
			if ( typeof url === "object" ) {
				options = url;
				url = undefined;
			}

			// Force options to be an object
			options = options || {};

			var transport,

				// URL without anti-cache param
				cacheURL,

				// Response headers
				responseHeadersString,
				responseHeaders,

				// timeout handle
				timeoutTimer,

				// Url cleanup var
				urlAnchor,

				// Request state (becomes false upon send and true upon completion)
				completed,

				// To know if global events are to be dispatched
				fireGlobals,

				// Loop variable
				i,

				// uncached part of the url
				uncached,

				// Create the final options object
				s = jQuery.ajaxSetup( {}, options ),

				// Callbacks context
				callbackContext = s.context || s,

				// Context for global events is callbackContext if it is a DOM node or jQuery collection
				globalEventContext = s.context &&
					( callbackContext.nodeType || callbackContext.jquery ) ?
						jQuery( callbackContext ) :
						jQuery.event,

				// Deferreds
				deferred = jQuery.Deferred(),
				completeDeferred = jQuery.Callbacks( "once memory" ),

				// Status-dependent callbacks
				statusCode = s.statusCode || {},

				// Headers (they are sent all at once)
				requestHeaders = {},
				requestHeadersNames = {},

				// Default abort message
				strAbort = "canceled",

				// Fake xhr
				jqXHR = {
					readyState: 0,

					// Builds headers hashtable if needed
					getResponseHeader: function( key ) {
						var match;
						if ( completed ) {
							if ( !responseHeaders ) {
								responseHeaders = {};
								while ( ( match = rheaders.exec( responseHeadersString ) ) ) {
									responseHeaders[ match[ 1 ].toLowerCase() ] = match[ 2 ];
								}
							}
							match = responseHeaders[ key.toLowerCase() ];
						}
						return match == null ? null : match;
					},

					// Raw string
					getAllResponseHeaders: function() {
						return completed ? responseHeadersString : null;
					},

					// Caches the header
					setRequestHeader: function( name, value ) {
						if ( completed == null ) {
							name = requestHeadersNames[ name.toLowerCase() ] =
								requestHeadersNames[ name.toLowerCase() ] || name;
							requestHeaders[ name ] = value;
						}
						return this;
					},

					// Overrides response content-type header
					overrideMimeType: function( type ) {
						if ( completed == null ) {
							s.mimeType = type;
						}
						return this;
					},

					// Status-dependent callbacks
					statusCode: function( map ) {
						var code;
						if ( map ) {
							if ( completed ) {

								// Execute the appropriate callbacks
								jqXHR.always( map[ jqXHR.status ] );
							} else {

								// Lazy-add the new callbacks in a way that preserves old ones
								for ( code in map ) {
									statusCode[ code ] = [ statusCode[ code ], map[ code ] ];
								}
							}
						}
						return this;
					},

					// Cancel the request
					abort: function( statusText ) {
						var finalText = statusText || strAbort;
						if ( transport ) {
							transport.abort( finalText );
						}
						done( 0, finalText );
						return this;
					}
				};

			// Attach deferreds
			deferred.promise( jqXHR );

			// Add protocol if not provided (prefilters might expect it)
			// Handle falsy url in the settings object (#10093: consistency with old signature)
			// We also use the url parameter if available
			s.url = ( ( url || s.url || location.href ) + "" )
				.replace( rprotocol, location.protocol + "//" );

			// Alias method option to type as per ticket #12004
			s.type = options.method || options.type || s.method || s.type;

			// Extract dataTypes list
			s.dataTypes = ( s.dataType || "*" ).toLowerCase().match( rnotwhite ) || [ "" ];

			// A cross-domain request is in order when the origin doesn't match the current origin.
			if ( s.crossDomain == null ) {
				urlAnchor = document.createElement( "a" );

				// Support: IE <=8 - 11, Edge 12 - 13
				// IE throws exception on accessing the href property if url is malformed,
				// e.g. http://example.com:80x/
				try {
					urlAnchor.href = s.url;

					// Support: IE <=8 - 11 only
					// Anchor's host property isn't correctly set when s.url is relative
					urlAnchor.href = urlAnchor.href;
					s.crossDomain = originAnchor.protocol + "//" + originAnchor.host !==
						urlAnchor.protocol + "//" + urlAnchor.host;
				} catch ( e ) {

					// If there is an error parsing the URL, assume it is crossDomain,
					// it can be rejected by the transport if it is invalid
					s.crossDomain = true;
				}
			}

			// Convert data if not already a string
			if ( s.data && s.processData && typeof s.data !== "string" ) {
				s.data = jQuery.param( s.data, s.traditional );
			}

			// Apply prefilters
			inspectPrefiltersOrTransports( prefilters, s, options, jqXHR );

			// If request was aborted inside a prefilter, stop there
			if ( completed ) {
				return jqXHR;
			}

			// We can fire global events as of now if asked to
			// Don't fire events if jQuery.event is undefined in an AMD-usage scenario (#15118)
			fireGlobals = jQuery.event && s.global;

			// Watch for a new set of requests
			if ( fireGlobals && jQuery.active++ === 0 ) {
				jQuery.event.trigger( "ajaxStart" );
			}

			// Uppercase the type
			s.type = s.type.toUpperCase();

			// Determine if request has content
			s.hasContent = !rnoContent.test( s.type );

			// Save the URL in case we're toying with the If-Modified-Since
			// and/or If-None-Match header later on
			// Remove hash to simplify url manipulation
			cacheURL = s.url.replace( rhash, "" );

			// More options handling for requests with no content
			if ( !s.hasContent ) {

				// Remember the hash so we can put it back
				uncached = s.url.slice( cacheURL.length );

				// If data is available, append data to url
				if ( s.data ) {
					cacheURL += ( rquery.test( cacheURL ) ? "&" : "?" ) + s.data;

					// #9682: remove data so that it's not used in an eventual retry
					delete s.data;
				}

				// Add anti-cache in uncached url if needed
				if ( s.cache === false ) {
					cacheURL = cacheURL.replace( rts, "" );
					uncached = ( rquery.test( cacheURL ) ? "&" : "?" ) + "_=" + ( nonce++ ) + uncached;
				}

				// Put hash and anti-cache on the URL that will be requested (gh-1732)
				s.url = cacheURL + uncached;

			// Change '%20' to '+' if this is encoded form body content (gh-2658)
			} else if ( s.data && s.processData &&
				( s.contentType || "" ).indexOf( "application/x-www-form-urlencoded" ) === 0 ) {
				s.data = s.data.replace( r20, "+" );
			}

			// Set the If-Modified-Since and/or If-None-Match header, if in ifModified mode.
			if ( s.ifModified ) {
				if ( jQuery.lastModified[ cacheURL ] ) {
					jqXHR.setRequestHeader( "If-Modified-Since", jQuery.lastModified[ cacheURL ] );
				}
				if ( jQuery.etag[ cacheURL ] ) {
					jqXHR.setRequestHeader( "If-None-Match", jQuery.etag[ cacheURL ] );
				}
			}

			// Set the correct header, if data is being sent
			if ( s.data && s.hasContent && s.contentType !== false || options.contentType ) {
				jqXHR.setRequestHeader( "Content-Type", s.contentType );
			}

			// Set the Accepts header for the server, depending on the dataType
			jqXHR.setRequestHeader(
				"Accept",
				s.dataTypes[ 0 ] && s.accepts[ s.dataTypes[ 0 ] ] ?
					s.accepts[ s.dataTypes[ 0 ] ] +
						( s.dataTypes[ 0 ] !== "*" ? ", " + allTypes + "; q=0.01" : "" ) :
					s.accepts[ "*" ]
			);

			// Check for headers option
			for ( i in s.headers ) {
				jqXHR.setRequestHeader( i, s.headers[ i ] );
			}

			// Allow custom headers/mimetypes and early abort
			if ( s.beforeSend &&
				( s.beforeSend.call( callbackContext, jqXHR, s ) === false || completed ) ) {

				// Abort if not done already and return
				return jqXHR.abort();
			}

			// Aborting is no longer a cancellation
			strAbort = "abort";

			// Install callbacks on deferreds
			completeDeferred.add( s.complete );
			jqXHR.done( s.success );
			jqXHR.fail( s.error );

			// Get transport
			transport = inspectPrefiltersOrTransports( transports, s, options, jqXHR );

			// If no transport, we auto-abort
			if ( !transport ) {
				done( -1, "No Transport" );
			} else {
				jqXHR.readyState = 1;

				// Send global event
				if ( fireGlobals ) {
					globalEventContext.trigger( "ajaxSend", [ jqXHR, s ] );
				}

				// If request was aborted inside ajaxSend, stop there
				if ( completed ) {
					return jqXHR;
				}

				// Timeout
				if ( s.async && s.timeout > 0 ) {
					timeoutTimer = window.setTimeout( function() {
						jqXHR.abort( "timeout" );
					}, s.timeout );
				}

				try {
					completed = false;
					transport.send( requestHeaders, done );
				} catch ( e ) {

					// Rethrow post-completion exceptions
					if ( completed ) {
						throw e;
					}

					// Propagate others as results
					done( -1, e );
				}
			}

			// Callback for when everything is done
			function done( status, nativeStatusText, responses, headers ) {
				var isSuccess, success, error, response, modified,
					statusText = nativeStatusText;

				// Ignore repeat invocations
				if ( completed ) {
					return;
				}

				completed = true;

				// Clear timeout if it exists
				if ( timeoutTimer ) {
					window.clearTimeout( timeoutTimer );
				}

				// Dereference transport for early garbage collection
				// (no matter how long the jqXHR object will be used)
				transport = undefined;

				// Cache response headers
				responseHeadersString = headers || "";

				// Set readyState
				jqXHR.readyState = status > 0 ? 4 : 0;

				// Determine if successful
				isSuccess = status >= 200 && status < 300 || status === 304;

				// Get response data
				if ( responses ) {
					response = ajaxHandleResponses( s, jqXHR, responses );
				}

				// Convert no matter what (that way responseXXX fields are always set)
				response = ajaxConvert( s, response, jqXHR, isSuccess );

				// If successful, handle type chaining
				if ( isSuccess ) {

					// Set the If-Modified-Since and/or If-None-Match header, if in ifModified mode.
					if ( s.ifModified ) {
						modified = jqXHR.getResponseHeader( "Last-Modified" );
						if ( modified ) {
							jQuery.lastModified[ cacheURL ] = modified;
						}
						modified = jqXHR.getResponseHeader( "etag" );
						if ( modified ) {
							jQuery.etag[ cacheURL ] = modified;
						}
					}

					// if no content
					if ( status === 204 || s.type === "HEAD" ) {
						statusText = "nocontent";

					// if not modified
					} else if ( status === 304 ) {
						statusText = "notmodified";

					// If we have data, let's convert it
					} else {
						statusText = response.state;
						success = response.data;
						error = response.error;
						isSuccess = !error;
					}
				} else {

					// Extract error from statusText and normalize for non-aborts
					error = statusText;
					if ( status || !statusText ) {
						statusText = "error";
						if ( status < 0 ) {
							status = 0;
						}
					}
				}

				// Set data for the fake xhr object
				jqXHR.status = status;
				jqXHR.statusText = ( nativeStatusText || statusText ) + "";

				// Success/Error
				if ( isSuccess ) {
					deferred.resolveWith( callbackContext, [ success, statusText, jqXHR ] );
				} else {
					deferred.rejectWith( callbackContext, [ jqXHR, statusText, error ] );
				}

				// Status-dependent callbacks
				jqXHR.statusCode( statusCode );
				statusCode = undefined;

				if ( fireGlobals ) {
					globalEventContext.trigger( isSuccess ? "ajaxSuccess" : "ajaxError",
						[ jqXHR, s, isSuccess ? success : error ] );
				}

				// Complete
				completeDeferred.fireWith( callbackContext, [ jqXHR, statusText ] );

				if ( fireGlobals ) {
					globalEventContext.trigger( "ajaxComplete", [ jqXHR, s ] );

					// Handle the global AJAX counter
					if ( !( --jQuery.active ) ) {
						jQuery.event.trigger( "ajaxStop" );
					}
				}
			}

			return jqXHR;
		},

		getJSON: function( url, data, callback ) {
			return jQuery.get( url, data, callback, "json" );
		},

		getScript: function( url, callback ) {
			return jQuery.get( url, undefined, callback, "script" );
		}
	} );

	jQuery.each( [ "get", "post" ], function( i, method ) {
		jQuery[ method ] = function( url, data, callback, type ) {

			// Shift arguments if data argument was omitted
			if ( jQuery.isFunction( data ) ) {
				type = type || callback;
				callback = data;
				data = undefined;
			}

			// The url can be an options object (which then must have .url)
			return jQuery.ajax( jQuery.extend( {
				url: url,
				type: method,
				dataType: type,
				data: data,
				success: callback
			}, jQuery.isPlainObject( url ) && url ) );
		};
	} );


	jQuery._evalUrl = function( url ) {
		return jQuery.ajax( {
			url: url,

			// Make this explicit, since user can override this through ajaxSetup (#11264)
			type: "GET",
			dataType: "script",
			cache: true,
			async: false,
			global: false,
			"throws": true
		} );
	};


	jQuery.fn.extend( {
		wrapAll: function( html ) {
			var wrap;

			if ( this[ 0 ] ) {
				if ( jQuery.isFunction( html ) ) {
					html = html.call( this[ 0 ] );
				}

				// The elements to wrap the target around
				wrap = jQuery( html, this[ 0 ].ownerDocument ).eq( 0 ).clone( true );

				if ( this[ 0 ].parentNode ) {
					wrap.insertBefore( this[ 0 ] );
				}

				wrap.map( function() {
					var elem = this;

					while ( elem.firstElementChild ) {
						elem = elem.firstElementChild;
					}

					return elem;
				} ).append( this );
			}

			return this;
		},

		wrapInner: function( html ) {
			if ( jQuery.isFunction( html ) ) {
				return this.each( function( i ) {
					jQuery( this ).wrapInner( html.call( this, i ) );
				} );
			}

			return this.each( function() {
				var self = jQuery( this ),
					contents = self.contents();

				if ( contents.length ) {
					contents.wrapAll( html );

				} else {
					self.append( html );
				}
			} );
		},

		wrap: function( html ) {
			var isFunction = jQuery.isFunction( html );

			return this.each( function( i ) {
				jQuery( this ).wrapAll( isFunction ? html.call( this, i ) : html );
			} );
		},

		unwrap: function( selector ) {
			this.parent( selector ).not( "body" ).each( function() {
				jQuery( this ).replaceWith( this.childNodes );
			} );
			return this;
		}
	} );


	jQuery.expr.pseudos.hidden = function( elem ) {
		return !jQuery.expr.pseudos.visible( elem );
	};
	jQuery.expr.pseudos.visible = function( elem ) {
		return !!( elem.offsetWidth || elem.offsetHeight || elem.getClientRects().length );
	};




	jQuery.ajaxSettings.xhr = function() {
		try {
			return new window.XMLHttpRequest();
		} catch ( e ) {}
	};

	var xhrSuccessStatus = {

			// File protocol always yields status code 0, assume 200
			0: 200,

			// Support: IE <=9 only
			// #1450: sometimes IE returns 1223 when it should be 204
			1223: 204
		},
		xhrSupported = jQuery.ajaxSettings.xhr();

	support.cors = !!xhrSupported && ( "withCredentials" in xhrSupported );
	support.ajax = xhrSupported = !!xhrSupported;

	jQuery.ajaxTransport( function( options ) {
		var callback, errorCallback;

		// Cross domain only allowed if supported through XMLHttpRequest
		if ( support.cors || xhrSupported && !options.crossDomain ) {
			return {
				send: function( headers, complete ) {
					var i,
						xhr = options.xhr();

					xhr.open(
						options.type,
						options.url,
						options.async,
						options.username,
						options.password
					);

					// Apply custom fields if provided
					if ( options.xhrFields ) {
						for ( i in options.xhrFields ) {
							xhr[ i ] = options.xhrFields[ i ];
						}
					}

					// Override mime type if needed
					if ( options.mimeType && xhr.overrideMimeType ) {
						xhr.overrideMimeType( options.mimeType );
					}

					// X-Requested-With header
					// For cross-domain requests, seeing as conditions for a preflight are
					// akin to a jigsaw puzzle, we simply never set it to be sure.
					// (it can always be set on a per-request basis or even using ajaxSetup)
					// For same-domain requests, won't change header if already provided.
					if ( !options.crossDomain && !headers[ "X-Requested-With" ] ) {
						headers[ "X-Requested-With" ] = "XMLHttpRequest";
					}

					// Set headers
					for ( i in headers ) {
						xhr.setRequestHeader( i, headers[ i ] );
					}

					// Callback
					callback = function( type ) {
						return function() {
							if ( callback ) {
								callback = errorCallback = xhr.onload =
									xhr.onerror = xhr.onabort = xhr.onreadystatechange = null;

								if ( type === "abort" ) {
									xhr.abort();
								} else if ( type === "error" ) {

									// Support: IE <=9 only
									// On a manual native abort, IE9 throws
									// errors on any property access that is not readyState
									if ( typeof xhr.status !== "number" ) {
										complete( 0, "error" );
									} else {
										complete(

											// File: protocol always yields status 0; see #8605, #14207
											xhr.status,
											xhr.statusText
										);
									}
								} else {
									complete(
										xhrSuccessStatus[ xhr.status ] || xhr.status,
										xhr.statusText,

										// Support: IE <=9 only
										// IE9 has no XHR2 but throws on binary (trac-11426)
										// For XHR2 non-text, let the caller handle it (gh-2498)
										( xhr.responseType || "text" ) !== "text"  ||
										typeof xhr.responseText !== "string" ?
											{ binary: xhr.response } :
											{ text: xhr.responseText },
										xhr.getAllResponseHeaders()
									);
								}
							}
						};
					};

					// Listen to events
					xhr.onload = callback();
					errorCallback = xhr.onerror = callback( "error" );

					// Support: IE 9 only
					// Use onreadystatechange to replace onabort
					// to handle uncaught aborts
					if ( xhr.onabort !== undefined ) {
						xhr.onabort = errorCallback;
					} else {
						xhr.onreadystatechange = function() {

							// Check readyState before timeout as it changes
							if ( xhr.readyState === 4 ) {

								// Allow onerror to be called first,
								// but that will not handle a native abort
								// Also, save errorCallback to a variable
								// as xhr.onerror cannot be accessed
								window.setTimeout( function() {
									if ( callback ) {
										errorCallback();
									}
								} );
							}
						};
					}

					// Create the abort callback
					callback = callback( "abort" );

					try {

						// Do send the request (this may raise an exception)
						xhr.send( options.hasContent && options.data || null );
					} catch ( e ) {

						// #14683: Only rethrow if this hasn't been notified as an error yet
						if ( callback ) {
							throw e;
						}
					}
				},

				abort: function() {
					if ( callback ) {
						callback();
					}
				}
			};
		}
	} );




	// Prevent auto-execution of scripts when no explicit dataType was provided (See gh-2432)
	jQuery.ajaxPrefilter( function( s ) {
		if ( s.crossDomain ) {
			s.contents.script = false;
		}
	} );

	// Install script dataType
	jQuery.ajaxSetup( {
		accepts: {
			script: "text/javascript, application/javascript, " +
				"application/ecmascript, application/x-ecmascript"
		},
		contents: {
			script: /\b(?:java|ecma)script\b/
		},
		converters: {
			"text script": function( text ) {
				jQuery.globalEval( text );
				return text;
			}
		}
	} );

	// Handle cache's special case and crossDomain
	jQuery.ajaxPrefilter( "script", function( s ) {
		if ( s.cache === undefined ) {
			s.cache = false;
		}
		if ( s.crossDomain ) {
			s.type = "GET";
		}
	} );

	// Bind script tag hack transport
	jQuery.ajaxTransport( "script", function( s ) {

		// This transport only deals with cross domain requests
		if ( s.crossDomain ) {
			var script, callback;
			return {
				send: function( _, complete ) {
					script = jQuery( "<script>" ).prop( {
						charset: s.scriptCharset,
						src: s.url
					} ).on(
						"load error",
						callback = function( evt ) {
							script.remove();
							callback = null;
							if ( evt ) {
								complete( evt.type === "error" ? 404 : 200, evt.type );
							}
						}
					);

					// Use native DOM manipulation to avoid our domManip AJAX trickery
					document.head.appendChild( script[ 0 ] );
				},
				abort: function() {
					if ( callback ) {
						callback();
					}
				}
			};
		}
	} );




	var oldCallbacks = [],
		rjsonp = /(=)\?(?=&|$)|\?\?/;

	// Default jsonp settings
	jQuery.ajaxSetup( {
		jsonp: "callback",
		jsonpCallback: function() {
			var callback = oldCallbacks.pop() || ( jQuery.expando + "_" + ( nonce++ ) );
			this[ callback ] = true;
			return callback;
		}
	} );

	// Detect, normalize options and install callbacks for jsonp requests
	jQuery.ajaxPrefilter( "json jsonp", function( s, originalSettings, jqXHR ) {

		var callbackName, overwritten, responseContainer,
			jsonProp = s.jsonp !== false && ( rjsonp.test( s.url ) ?
				"url" :
				typeof s.data === "string" &&
					( s.contentType || "" )
						.indexOf( "application/x-www-form-urlencoded" ) === 0 &&
					rjsonp.test( s.data ) && "data"
			);

		// Handle iff the expected data type is "jsonp" or we have a parameter to set
		if ( jsonProp || s.dataTypes[ 0 ] === "jsonp" ) {

			// Get callback name, remembering preexisting value associated with it
			callbackName = s.jsonpCallback = jQuery.isFunction( s.jsonpCallback ) ?
				s.jsonpCallback() :
				s.jsonpCallback;

			// Insert callback into url or form data
			if ( jsonProp ) {
				s[ jsonProp ] = s[ jsonProp ].replace( rjsonp, "$1" + callbackName );
			} else if ( s.jsonp !== false ) {
				s.url += ( rquery.test( s.url ) ? "&" : "?" ) + s.jsonp + "=" + callbackName;
			}

			// Use data converter to retrieve json after script execution
			s.converters[ "script json" ] = function() {
				if ( !responseContainer ) {
					jQuery.error( callbackName + " was not called" );
				}
				return responseContainer[ 0 ];
			};

			// Force json dataType
			s.dataTypes[ 0 ] = "json";

			// Install callback
			overwritten = window[ callbackName ];
			window[ callbackName ] = function() {
				responseContainer = arguments;
			};

			// Clean-up function (fires after converters)
			jqXHR.always( function() {

				// If previous value didn't exist - remove it
				if ( overwritten === undefined ) {
					jQuery( window ).removeProp( callbackName );

				// Otherwise restore preexisting value
				} else {
					window[ callbackName ] = overwritten;
				}

				// Save back as free
				if ( s[ callbackName ] ) {

					// Make sure that re-using the options doesn't screw things around
					s.jsonpCallback = originalSettings.jsonpCallback;

					// Save the callback name for future use
					oldCallbacks.push( callbackName );
				}

				// Call if it was a function and we have a response
				if ( responseContainer && jQuery.isFunction( overwritten ) ) {
					overwritten( responseContainer[ 0 ] );
				}

				responseContainer = overwritten = undefined;
			} );

			// Delegate to script
			return "script";
		}
	} );




	// Support: Safari 8 only
	// In Safari 8 documents created via document.implementation.createHTMLDocument
	// collapse sibling forms: the second one becomes a child of the first one.
	// Because of that, this security measure has to be disabled in Safari 8.
	// https://bugs.webkit.org/show_bug.cgi?id=137337
	support.createHTMLDocument = ( function() {
		var body = document.implementation.createHTMLDocument( "" ).body;
		body.innerHTML = "<form></form><form></form>";
		return body.childNodes.length === 2;
	} )();


	// Argument "data" should be string of html
	// context (optional): If specified, the fragment will be created in this context,
	// defaults to document
	// keepScripts (optional): If true, will include scripts passed in the html string
	jQuery.parseHTML = function( data, context, keepScripts ) {
		if ( typeof data !== "string" ) {
			return [];
		}
		if ( typeof context === "boolean" ) {
			keepScripts = context;
			context = false;
		}

		var base, parsed, scripts;

		if ( !context ) {

			// Stop scripts or inline event handlers from being executed immediately
			// by using document.implementation
			if ( support.createHTMLDocument ) {
				context = document.implementation.createHTMLDocument( "" );

				// Set the base href for the created document
				// so any parsed elements with URLs
				// are based on the document's URL (gh-2965)
				base = context.createElement( "base" );
				base.href = document.location.href;
				context.head.appendChild( base );
			} else {
				context = document;
			}
		}

		parsed = rsingleTag.exec( data );
		scripts = !keepScripts && [];

		// Single tag
		if ( parsed ) {
			return [ context.createElement( parsed[ 1 ] ) ];
		}

		parsed = buildFragment( [ data ], context, scripts );

		if ( scripts && scripts.length ) {
			jQuery( scripts ).remove();
		}

		return jQuery.merge( [], parsed.childNodes );
	};


	/**
	 * Load a url into a page
	 */
	jQuery.fn.load = function( url, params, callback ) {
		var selector, type, response,
			self = this,
			off = url.indexOf( " " );

		if ( off > -1 ) {
			selector = jQuery.trim( url.slice( off ) );
			url = url.slice( 0, off );
		}

		// If it's a function
		if ( jQuery.isFunction( params ) ) {

			// We assume that it's the callback
			callback = params;
			params = undefined;

		// Otherwise, build a param string
		} else if ( params && typeof params === "object" ) {
			type = "POST";
		}

		// If we have elements to modify, make the request
		if ( self.length > 0 ) {
			jQuery.ajax( {
				url: url,

				// If "type" variable is undefined, then "GET" method will be used.
				// Make value of this field explicit since
				// user can override it through ajaxSetup method
				type: type || "GET",
				dataType: "html",
				data: params
			} ).done( function( responseText ) {

				// Save response for use in complete callback
				response = arguments;

				self.html( selector ?

					// If a selector was specified, locate the right elements in a dummy div
					// Exclude scripts to avoid IE 'Permission Denied' errors
					jQuery( "<div>" ).append( jQuery.parseHTML( responseText ) ).find( selector ) :

					// Otherwise use the full result
					responseText );

			// If the request succeeds, this function gets "data", "status", "jqXHR"
			// but they are ignored because response was set above.
			// If it fails, this function gets "jqXHR", "status", "error"
			} ).always( callback && function( jqXHR, status ) {
				self.each( function() {
					callback.apply( this, response || [ jqXHR.responseText, status, jqXHR ] );
				} );
			} );
		}

		return this;
	};




	// Attach a bunch of functions for handling common AJAX events
	jQuery.each( [
		"ajaxStart",
		"ajaxStop",
		"ajaxComplete",
		"ajaxError",
		"ajaxSuccess",
		"ajaxSend"
	], function( i, type ) {
		jQuery.fn[ type ] = function( fn ) {
			return this.on( type, fn );
		};
	} );




	jQuery.expr.pseudos.animated = function( elem ) {
		return jQuery.grep( jQuery.timers, function( fn ) {
			return elem === fn.elem;
		} ).length;
	};




	/**
	 * Gets a window from an element
	 */
	function getWindow( elem ) {
		return jQuery.isWindow( elem ) ? elem : elem.nodeType === 9 && elem.defaultView;
	}

	jQuery.offset = {
		setOffset: function( elem, options, i ) {
			var curPosition, curLeft, curCSSTop, curTop, curOffset, curCSSLeft, calculatePosition,
				position = jQuery.css( elem, "position" ),
				curElem = jQuery( elem ),
				props = {};

			// Set position first, in-case top/left are set even on static elem
			if ( position === "static" ) {
				elem.style.position = "relative";
			}

			curOffset = curElem.offset();
			curCSSTop = jQuery.css( elem, "top" );
			curCSSLeft = jQuery.css( elem, "left" );
			calculatePosition = ( position === "absolute" || position === "fixed" ) &&
				( curCSSTop + curCSSLeft ).indexOf( "auto" ) > -1;

			// Need to be able to calculate position if either
			// top or left is auto and position is either absolute or fixed
			if ( calculatePosition ) {
				curPosition = curElem.position();
				curTop = curPosition.top;
				curLeft = curPosition.left;

			} else {
				curTop = parseFloat( curCSSTop ) || 0;
				curLeft = parseFloat( curCSSLeft ) || 0;
			}

			if ( jQuery.isFunction( options ) ) {

				// Use jQuery.extend here to allow modification of coordinates argument (gh-1848)
				options = options.call( elem, i, jQuery.extend( {}, curOffset ) );
			}

			if ( options.top != null ) {
				props.top = ( options.top - curOffset.top ) + curTop;
			}
			if ( options.left != null ) {
				props.left = ( options.left - curOffset.left ) + curLeft;
			}

			if ( "using" in options ) {
				options.using.call( elem, props );

			} else {
				curElem.css( props );
			}
		}
	};

	jQuery.fn.extend( {
		offset: function( options ) {

			// Preserve chaining for setter
			if ( arguments.length ) {
				return options === undefined ?
					this :
					this.each( function( i ) {
						jQuery.offset.setOffset( this, options, i );
					} );
			}

			var docElem, win, rect, doc,
				elem = this[ 0 ];

			if ( !elem ) {
				return;
			}

			// Support: IE <=11 only
			// Running getBoundingClientRect on a
			// disconnected node in IE throws an error
			if ( !elem.getClientRects().length ) {
				return { top: 0, left: 0 };
			}

			rect = elem.getBoundingClientRect();

			// Make sure element is not hidden (display: none)
			if ( rect.width || rect.height ) {
				doc = elem.ownerDocument;
				win = getWindow( doc );
				docElem = doc.documentElement;

				return {
					top: rect.top + win.pageYOffset - docElem.clientTop,
					left: rect.left + win.pageXOffset - docElem.clientLeft
				};
			}

			// Return zeros for disconnected and hidden elements (gh-2310)
			return rect;
		},

		position: function() {
			if ( !this[ 0 ] ) {
				return;
			}

			var offsetParent, offset,
				elem = this[ 0 ],
				parentOffset = { top: 0, left: 0 };

			// Fixed elements are offset from window (parentOffset = {top:0, left: 0},
			// because it is its only offset parent
			if ( jQuery.css( elem, "position" ) === "fixed" ) {

				// Assume getBoundingClientRect is there when computed position is fixed
				offset = elem.getBoundingClientRect();

			} else {

				// Get *real* offsetParent
				offsetParent = this.offsetParent();

				// Get correct offsets
				offset = this.offset();
				if ( !jQuery.nodeName( offsetParent[ 0 ], "html" ) ) {
					parentOffset = offsetParent.offset();
				}

				// Add offsetParent borders
				parentOffset = {
					top: parentOffset.top + jQuery.css( offsetParent[ 0 ], "borderTopWidth", true ),
					left: parentOffset.left + jQuery.css( offsetParent[ 0 ], "borderLeftWidth", true )
				};
			}

			// Subtract parent offsets and element margins
			return {
				top: offset.top - parentOffset.top - jQuery.css( elem, "marginTop", true ),
				left: offset.left - parentOffset.left - jQuery.css( elem, "marginLeft", true )
			};
		},

		// This method will return documentElement in the following cases:
		// 1) For the element inside the iframe without offsetParent, this method will return
		//    documentElement of the parent window
		// 2) For the hidden or detached element
		// 3) For body or html element, i.e. in case of the html node - it will return itself
		//
		// but those exceptions were never presented as a real life use-cases
		// and might be considered as more preferable results.
		//
		// This logic, however, is not guaranteed and can change at any point in the future
		offsetParent: function() {
			return this.map( function() {
				var offsetParent = this.offsetParent;

				while ( offsetParent && jQuery.css( offsetParent, "position" ) === "static" ) {
					offsetParent = offsetParent.offsetParent;
				}

				return offsetParent || documentElement;
			} );
		}
	} );

	// Create scrollLeft and scrollTop methods
	jQuery.each( { scrollLeft: "pageXOffset", scrollTop: "pageYOffset" }, function( method, prop ) {
		var top = "pageYOffset" === prop;

		jQuery.fn[ method ] = function( val ) {
			return access( this, function( elem, method, val ) {
				var win = getWindow( elem );

				if ( val === undefined ) {
					return win ? win[ prop ] : elem[ method ];
				}

				if ( win ) {
					win.scrollTo(
						!top ? val : win.pageXOffset,
						top ? val : win.pageYOffset
					);

				} else {
					elem[ method ] = val;
				}
			}, method, val, arguments.length );
		};
	} );

	// Support: Safari <=7 - 9.1, Chrome <=37 - 49
	// Add the top/left cssHooks using jQuery.fn.position
	// Webkit bug: https://bugs.webkit.org/show_bug.cgi?id=29084
	// Blink bug: https://bugs.chromium.org/p/chromium/issues/detail?id=589347
	// getComputedStyle returns percent when specified for top/left/bottom/right;
	// rather than make the css module depend on the offset module, just check for it here
	jQuery.each( [ "top", "left" ], function( i, prop ) {
		jQuery.cssHooks[ prop ] = addGetHookIf( support.pixelPosition,
			function( elem, computed ) {
				if ( computed ) {
					computed = curCSS( elem, prop );

					// If curCSS returns percentage, fallback to offset
					return rnumnonpx.test( computed ) ?
						jQuery( elem ).position()[ prop ] + "px" :
						computed;
				}
			}
		);
	} );


	// Create innerHeight, innerWidth, height, width, outerHeight and outerWidth methods
	jQuery.each( { Height: "height", Width: "width" }, function( name, type ) {
		jQuery.each( { padding: "inner" + name, content: type, "": "outer" + name },
			function( defaultExtra, funcName ) {

			// Margin is only for outerHeight, outerWidth
			jQuery.fn[ funcName ] = function( margin, value ) {
				var chainable = arguments.length && ( defaultExtra || typeof margin !== "boolean" ),
					extra = defaultExtra || ( margin === true || value === true ? "margin" : "border" );

				return access( this, function( elem, type, value ) {
					var doc;

					if ( jQuery.isWindow( elem ) ) {

						// $( window ).outerWidth/Height return w/h including scrollbars (gh-1729)
						return funcName.indexOf( "outer" ) === 0 ?
							elem[ "inner" + name ] :
							elem.document.documentElement[ "client" + name ];
					}

					// Get document width or height
					if ( elem.nodeType === 9 ) {
						doc = elem.documentElement;

						// Either scroll[Width/Height] or offset[Width/Height] or client[Width/Height],
						// whichever is greatest
						return Math.max(
							elem.body[ "scroll" + name ], doc[ "scroll" + name ],
							elem.body[ "offset" + name ], doc[ "offset" + name ],
							doc[ "client" + name ]
						);
					}

					return value === undefined ?

						// Get width or height on the element, requesting but not forcing parseFloat
						jQuery.css( elem, type, extra ) :

						// Set width or height on the element
						jQuery.style( elem, type, value, extra );
				}, type, chainable ? margin : undefined, chainable );
			};
		} );
	} );


	jQuery.fn.extend( {

		bind: function( types, data, fn ) {
			return this.on( types, null, data, fn );
		},
		unbind: function( types, fn ) {
			return this.off( types, null, fn );
		},

		delegate: function( selector, types, data, fn ) {
			return this.on( types, selector, data, fn );
		},
		undelegate: function( selector, types, fn ) {

			// ( namespace ) or ( selector, types [, fn] )
			return arguments.length === 1 ?
				this.off( selector, "**" ) :
				this.off( types, selector || "**", fn );
		}
	} );

	jQuery.parseJSON = JSON.parse;




	// Register as a named AMD module, since jQuery can be concatenated with other
	// files that may use define, but not via a proper concatenation script that
	// understands anonymous AMD modules. A named AMD is safest and most robust
	// way to register. Lowercase jquery is used because AMD module names are
	// derived from file names, and jQuery is normally delivered in a lowercase
	// file name. Do this after creating the global so that if an AMD module wants
	// to call noConflict to hide this version of jQuery, it will work.

	// Note that for maximum portability, libraries that are not jQuery should
	// declare themselves as anonymous modules, and avoid setting a global if an
	// AMD loader is present. jQuery is a special case. For more information, see
	// https://github.com/jrburke/requirejs/wiki/Updating-existing-libraries#wiki-anon

	if ( true ) {
		!(__WEBPACK_AMD_DEFINE_ARRAY__ = [], __WEBPACK_AMD_DEFINE_RESULT__ = function() {
			return jQuery;
		}.apply(exports, __WEBPACK_AMD_DEFINE_ARRAY__), __WEBPACK_AMD_DEFINE_RESULT__ !== undefined && (module.exports = __WEBPACK_AMD_DEFINE_RESULT__));
	}





	var

		// Map over jQuery in case of overwrite
		_jQuery = window.jQuery,

		// Map over the $ in case of overwrite
		_$ = window.$;

	jQuery.noConflict = function( deep ) {
		if ( window.$ === jQuery ) {
			window.$ = _$;
		}

		if ( deep && window.jQuery === jQuery ) {
			window.jQuery = _jQuery;
		}

		return jQuery;
	};

	// Expose jQuery and $ identifiers, even in AMD
	// (#7102#comment:10, https://github.com/jquery/jquery/pull/557)
	// and CommonJS for browser emulators (#13566)
	if ( !noGlobal ) {
		window.jQuery = window.$ = jQuery;
	}


	return jQuery;
	} );


/***/ },
/* 2 */
/***/ function(module, exports, __webpack_require__) {

	// style-loader: Adds some css to the DOM by adding a <style> tag

	// load the styles
	var content = __webpack_require__(3);
	if(typeof content === 'string') content = [[module.id, content, '']];
	// add the styles to the DOM
	var update = __webpack_require__(10)(content, {});
	if(content.locals) module.exports = content.locals;
	// Hot Module Replacement
	if(false) {
		// When the styles change, update the <style> tags
		if(!content.locals) {
			module.hot.accept("!!./../../../css-loader/index.js!./bootstrap.min.css", function() {
				var newContent = require("!!./../../../css-loader/index.js!./bootstrap.min.css");
				if(typeof newContent === 'string') newContent = [[module.id, newContent, '']];
				update(newContent);
			});
		}
		// When the module is disposed, remove the <style> tags
		module.hot.dispose(function() { update(); });
	}

/***/ },
/* 3 */
/***/ function(module, exports, __webpack_require__) {

	exports = module.exports = __webpack_require__(4)();
	// imports


	// module
	exports.push([module.id, "/*!\n * Bootstrap v3.3.6 (http://getbootstrap.com)\n * Copyright 2011-2015 Twitter, Inc.\n * Licensed under MIT (https://github.com/twbs/bootstrap/blob/master/LICENSE)\n *//*! normalize.css v3.0.3 | MIT License | github.com/necolas/normalize.css */html{font-family:sans-serif;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%}body{margin:0}article,aside,details,figcaption,figure,footer,header,hgroup,main,menu,nav,section,summary{display:block}audio,canvas,progress,video{display:inline-block;vertical-align:baseline}audio:not([controls]){display:none;height:0}[hidden],template{display:none}a{background-color:transparent}a:active,a:hover{outline:0}abbr[title]{border-bottom:1px dotted}b,strong{font-weight:700}dfn{font-style:italic}h1{margin:.67em 0;font-size:2em}mark{color:#000;background:#ff0}small{font-size:80%}sub,sup{position:relative;font-size:75%;line-height:0;vertical-align:baseline}sup{top:-.5em}sub{bottom:-.25em}img{border:0}svg:not(:root){overflow:hidden}figure{margin:1em 40px}hr{height:0;-webkit-box-sizing:content-box;-moz-box-sizing:content-box;box-sizing:content-box}pre{overflow:auto}code,kbd,pre,samp{font-family:monospace,monospace;font-size:1em}button,input,optgroup,select,textarea{margin:0;font:inherit;color:inherit}button{overflow:visible}button,select{text-transform:none}button,html input[type=button],input[type=reset],input[type=submit]{-webkit-appearance:button;cursor:pointer}button[disabled],html input[disabled]{cursor:default}button::-moz-focus-inner,input::-moz-focus-inner{padding:0;border:0}input{line-height:normal}input[type=checkbox],input[type=radio]{-webkit-box-sizing:border-box;-moz-box-sizing:border-box;box-sizing:border-box;padding:0}input[type=number]::-webkit-inner-spin-button,input[type=number]::-webkit-outer-spin-button{height:auto}input[type=search]{-webkit-box-sizing:content-box;-moz-box-sizing:content-box;box-sizing:content-box;-webkit-appearance:textfield}input[type=search]::-webkit-search-cancel-button,input[type=search]::-webkit-search-decoration{-webkit-appearance:none}fieldset{padding:.35em .625em .75em;margin:0 2px;border:1px solid silver}legend{padding:0;border:0}textarea{overflow:auto}optgroup{font-weight:700}table{border-spacing:0;border-collapse:collapse}td,th{padding:0}/*! Source: https://github.com/h5bp/html5-boilerplate/blob/master/src/css/main.css */@media print{*,:after,:before{color:#000!important;text-shadow:none!important;background:0 0!important;-webkit-box-shadow:none!important;box-shadow:none!important}a,a:visited{text-decoration:underline}a[href]:after{content:\" (\" attr(href) \")\"}abbr[title]:after{content:\" (\" attr(title) \")\"}a[href^=\"javascript:\"]:after,a[href^=\"#\"]:after{content:\"\"}blockquote,pre{border:1px solid #999;page-break-inside:avoid}thead{display:table-header-group}img,tr{page-break-inside:avoid}img{max-width:100%!important}h2,h3,p{orphans:3;widows:3}h2,h3{page-break-after:avoid}.navbar{display:none}.btn>.caret,.dropup>.btn>.caret{border-top-color:#000!important}.label{border:1px solid #000}.table{border-collapse:collapse!important}.table td,.table th{background-color:#fff!important}.table-bordered td,.table-bordered th{border:1px solid #ddd!important}}@font-face{font-family:'Glyphicons Halflings';src:url(" + __webpack_require__(5) + ");src:url(" + __webpack_require__(5) + "?#iefix) format('embedded-opentype'),url(" + __webpack_require__(6) + ") format('woff2'),url(" + __webpack_require__(7) + ") format('woff'),url(" + __webpack_require__(8) + ") format('truetype'),url(" + __webpack_require__(9) + "#glyphicons_halflingsregular) format('svg')}.glyphicon{position:relative;top:1px;display:inline-block;font-family:'Glyphicons Halflings';font-style:normal;font-weight:400;line-height:1;-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale}.glyphicon-asterisk:before{content:\"*\"}.glyphicon-plus:before{content:\"+\"}.glyphicon-eur:before,.glyphicon-euro:before{content:\"\\20AC\"}.glyphicon-minus:before{content:\"\\2212\"}.glyphicon-cloud:before{content:\"\\2601\"}.glyphicon-envelope:before{content:\"\\2709\"}.glyphicon-pencil:before{content:\"\\270F\"}.glyphicon-glass:before{content:\"\\E001\"}.glyphicon-music:before{content:\"\\E002\"}.glyphicon-search:before{content:\"\\E003\"}.glyphicon-heart:before{content:\"\\E005\"}.glyphicon-star:before{content:\"\\E006\"}.glyphicon-star-empty:before{content:\"\\E007\"}.glyphicon-user:before{content:\"\\E008\"}.glyphicon-film:before{content:\"\\E009\"}.glyphicon-th-large:before{content:\"\\E010\"}.glyphicon-th:before{content:\"\\E011\"}.glyphicon-th-list:before{content:\"\\E012\"}.glyphicon-ok:before{content:\"\\E013\"}.glyphicon-remove:before{content:\"\\E014\"}.glyphicon-zoom-in:before{content:\"\\E015\"}.glyphicon-zoom-out:before{content:\"\\E016\"}.glyphicon-off:before{content:\"\\E017\"}.glyphicon-signal:before{content:\"\\E018\"}.glyphicon-cog:before{content:\"\\E019\"}.glyphicon-trash:before{content:\"\\E020\"}.glyphicon-home:before{content:\"\\E021\"}.glyphicon-file:before{content:\"\\E022\"}.glyphicon-time:before{content:\"\\E023\"}.glyphicon-road:before{content:\"\\E024\"}.glyphicon-download-alt:before{content:\"\\E025\"}.glyphicon-download:before{content:\"\\E026\"}.glyphicon-upload:before{content:\"\\E027\"}.glyphicon-inbox:before{content:\"\\E028\"}.glyphicon-play-circle:before{content:\"\\E029\"}.glyphicon-repeat:before{content:\"\\E030\"}.glyphicon-refresh:before{content:\"\\E031\"}.glyphicon-list-alt:before{content:\"\\E032\"}.glyphicon-lock:before{content:\"\\E033\"}.glyphicon-flag:before{content:\"\\E034\"}.glyphicon-headphones:before{content:\"\\E035\"}.glyphicon-volume-off:before{content:\"\\E036\"}.glyphicon-volume-down:before{content:\"\\E037\"}.glyphicon-volume-up:before{content:\"\\E038\"}.glyphicon-qrcode:before{content:\"\\E039\"}.glyphicon-barcode:before{content:\"\\E040\"}.glyphicon-tag:before{content:\"\\E041\"}.glyphicon-tags:before{content:\"\\E042\"}.glyphicon-book:before{content:\"\\E043\"}.glyphicon-bookmark:before{content:\"\\E044\"}.glyphicon-print:before{content:\"\\E045\"}.glyphicon-camera:before{content:\"\\E046\"}.glyphicon-font:before{content:\"\\E047\"}.glyphicon-bold:before{content:\"\\E048\"}.glyphicon-italic:before{content:\"\\E049\"}.glyphicon-text-height:before{content:\"\\E050\"}.glyphicon-text-width:before{content:\"\\E051\"}.glyphicon-align-left:before{content:\"\\E052\"}.glyphicon-align-center:before{content:\"\\E053\"}.glyphicon-align-right:before{content:\"\\E054\"}.glyphicon-align-justify:before{content:\"\\E055\"}.glyphicon-list:before{content:\"\\E056\"}.glyphicon-indent-left:before{content:\"\\E057\"}.glyphicon-indent-right:before{content:\"\\E058\"}.glyphicon-facetime-video:before{content:\"\\E059\"}.glyphicon-picture:before{content:\"\\E060\"}.glyphicon-map-marker:before{content:\"\\E062\"}.glyphicon-adjust:before{content:\"\\E063\"}.glyphicon-tint:before{content:\"\\E064\"}.glyphicon-edit:before{content:\"\\E065\"}.glyphicon-share:before{content:\"\\E066\"}.glyphicon-check:before{content:\"\\E067\"}.glyphicon-move:before{content:\"\\E068\"}.glyphicon-step-backward:before{content:\"\\E069\"}.glyphicon-fast-backward:before{content:\"\\E070\"}.glyphicon-backward:before{content:\"\\E071\"}.glyphicon-play:before{content:\"\\E072\"}.glyphicon-pause:before{content:\"\\E073\"}.glyphicon-stop:before{content:\"\\E074\"}.glyphicon-forward:before{content:\"\\E075\"}.glyphicon-fast-forward:before{content:\"\\E076\"}.glyphicon-step-forward:before{content:\"\\E077\"}.glyphicon-eject:before{content:\"\\E078\"}.glyphicon-chevron-left:before{content:\"\\E079\"}.glyphicon-chevron-right:before{content:\"\\E080\"}.glyphicon-plus-sign:before{content:\"\\E081\"}.glyphicon-minus-sign:before{content:\"\\E082\"}.glyphicon-remove-sign:before{content:\"\\E083\"}.glyphicon-ok-sign:before{content:\"\\E084\"}.glyphicon-question-sign:before{content:\"\\E085\"}.glyphicon-info-sign:before{content:\"\\E086\"}.glyphicon-screenshot:before{content:\"\\E087\"}.glyphicon-remove-circle:before{content:\"\\E088\"}.glyphicon-ok-circle:before{content:\"\\E089\"}.glyphicon-ban-circle:before{content:\"\\E090\"}.glyphicon-arrow-left:before{content:\"\\E091\"}.glyphicon-arrow-right:before{content:\"\\E092\"}.glyphicon-arrow-up:before{content:\"\\E093\"}.glyphicon-arrow-down:before{content:\"\\E094\"}.glyphicon-share-alt:before{content:\"\\E095\"}.glyphicon-resize-full:before{content:\"\\E096\"}.glyphicon-resize-small:before{content:\"\\E097\"}.glyphicon-exclamation-sign:before{content:\"\\E101\"}.glyphicon-gift:before{content:\"\\E102\"}.glyphicon-leaf:before{content:\"\\E103\"}.glyphicon-fire:before{content:\"\\E104\"}.glyphicon-eye-open:before{content:\"\\E105\"}.glyphicon-eye-close:before{content:\"\\E106\"}.glyphicon-warning-sign:before{content:\"\\E107\"}.glyphicon-plane:before{content:\"\\E108\"}.glyphicon-calendar:before{content:\"\\E109\"}.glyphicon-random:before{content:\"\\E110\"}.glyphicon-comment:before{content:\"\\E111\"}.glyphicon-magnet:before{content:\"\\E112\"}.glyphicon-chevron-up:before{content:\"\\E113\"}.glyphicon-chevron-down:before{content:\"\\E114\"}.glyphicon-retweet:before{content:\"\\E115\"}.glyphicon-shopping-cart:before{content:\"\\E116\"}.glyphicon-folder-close:before{content:\"\\E117\"}.glyphicon-folder-open:before{content:\"\\E118\"}.glyphicon-resize-vertical:before{content:\"\\E119\"}.glyphicon-resize-horizontal:before{content:\"\\E120\"}.glyphicon-hdd:before{content:\"\\E121\"}.glyphicon-bullhorn:before{content:\"\\E122\"}.glyphicon-bell:before{content:\"\\E123\"}.glyphicon-certificate:before{content:\"\\E124\"}.glyphicon-thumbs-up:before{content:\"\\E125\"}.glyphicon-thumbs-down:before{content:\"\\E126\"}.glyphicon-hand-right:before{content:\"\\E127\"}.glyphicon-hand-left:before{content:\"\\E128\"}.glyphicon-hand-up:before{content:\"\\E129\"}.glyphicon-hand-down:before{content:\"\\E130\"}.glyphicon-circle-arrow-right:before{content:\"\\E131\"}.glyphicon-circle-arrow-left:before{content:\"\\E132\"}.glyphicon-circle-arrow-up:before{content:\"\\E133\"}.glyphicon-circle-arrow-down:before{content:\"\\E134\"}.glyphicon-globe:before{content:\"\\E135\"}.glyphicon-wrench:before{content:\"\\E136\"}.glyphicon-tasks:before{content:\"\\E137\"}.glyphicon-filter:before{content:\"\\E138\"}.glyphicon-briefcase:before{content:\"\\E139\"}.glyphicon-fullscreen:before{content:\"\\E140\"}.glyphicon-dashboard:before{content:\"\\E141\"}.glyphicon-paperclip:before{content:\"\\E142\"}.glyphicon-heart-empty:before{content:\"\\E143\"}.glyphicon-link:before{content:\"\\E144\"}.glyphicon-phone:before{content:\"\\E145\"}.glyphicon-pushpin:before{content:\"\\E146\"}.glyphicon-usd:before{content:\"\\E148\"}.glyphicon-gbp:before{content:\"\\E149\"}.glyphicon-sort:before{content:\"\\E150\"}.glyphicon-sort-by-alphabet:before{content:\"\\E151\"}.glyphicon-sort-by-alphabet-alt:before{content:\"\\E152\"}.glyphicon-sort-by-order:before{content:\"\\E153\"}.glyphicon-sort-by-order-alt:before{content:\"\\E154\"}.glyphicon-sort-by-attributes:before{content:\"\\E155\"}.glyphicon-sort-by-attributes-alt:before{content:\"\\E156\"}.glyphicon-unchecked:before{content:\"\\E157\"}.glyphicon-expand:before{content:\"\\E158\"}.glyphicon-collapse-down:before{content:\"\\E159\"}.glyphicon-collapse-up:before{content:\"\\E160\"}.glyphicon-log-in:before{content:\"\\E161\"}.glyphicon-flash:before{content:\"\\E162\"}.glyphicon-log-out:before{content:\"\\E163\"}.glyphicon-new-window:before{content:\"\\E164\"}.glyphicon-record:before{content:\"\\E165\"}.glyphicon-save:before{content:\"\\E166\"}.glyphicon-open:before{content:\"\\E167\"}.glyphicon-saved:before{content:\"\\E168\"}.glyphicon-import:before{content:\"\\E169\"}.glyphicon-export:before{content:\"\\E170\"}.glyphicon-send:before{content:\"\\E171\"}.glyphicon-floppy-disk:before{content:\"\\E172\"}.glyphicon-floppy-saved:before{content:\"\\E173\"}.glyphicon-floppy-remove:before{content:\"\\E174\"}.glyphicon-floppy-save:before{content:\"\\E175\"}.glyphicon-floppy-open:before{content:\"\\E176\"}.glyphicon-credit-card:before{content:\"\\E177\"}.glyphicon-transfer:before{content:\"\\E178\"}.glyphicon-cutlery:before{content:\"\\E179\"}.glyphicon-header:before{content:\"\\E180\"}.glyphicon-compressed:before{content:\"\\E181\"}.glyphicon-earphone:before{content:\"\\E182\"}.glyphicon-phone-alt:before{content:\"\\E183\"}.glyphicon-tower:before{content:\"\\E184\"}.glyphicon-stats:before{content:\"\\E185\"}.glyphicon-sd-video:before{content:\"\\E186\"}.glyphicon-hd-video:before{content:\"\\E187\"}.glyphicon-subtitles:before{content:\"\\E188\"}.glyphicon-sound-stereo:before{content:\"\\E189\"}.glyphicon-sound-dolby:before{content:\"\\E190\"}.glyphicon-sound-5-1:before{content:\"\\E191\"}.glyphicon-sound-6-1:before{content:\"\\E192\"}.glyphicon-sound-7-1:before{content:\"\\E193\"}.glyphicon-copyright-mark:before{content:\"\\E194\"}.glyphicon-registration-mark:before{content:\"\\E195\"}.glyphicon-cloud-download:before{content:\"\\E197\"}.glyphicon-cloud-upload:before{content:\"\\E198\"}.glyphicon-tree-conifer:before{content:\"\\E199\"}.glyphicon-tree-deciduous:before{content:\"\\E200\"}.glyphicon-cd:before{content:\"\\E201\"}.glyphicon-save-file:before{content:\"\\E202\"}.glyphicon-open-file:before{content:\"\\E203\"}.glyphicon-level-up:before{content:\"\\E204\"}.glyphicon-copy:before{content:\"\\E205\"}.glyphicon-paste:before{content:\"\\E206\"}.glyphicon-alert:before{content:\"\\E209\"}.glyphicon-equalizer:before{content:\"\\E210\"}.glyphicon-king:before{content:\"\\E211\"}.glyphicon-queen:before{content:\"\\E212\"}.glyphicon-pawn:before{content:\"\\E213\"}.glyphicon-bishop:before{content:\"\\E214\"}.glyphicon-knight:before{content:\"\\E215\"}.glyphicon-baby-formula:before{content:\"\\E216\"}.glyphicon-tent:before{content:\"\\26FA\"}.glyphicon-blackboard:before{content:\"\\E218\"}.glyphicon-bed:before{content:\"\\E219\"}.glyphicon-apple:before{content:\"\\F8FF\"}.glyphicon-erase:before{content:\"\\E221\"}.glyphicon-hourglass:before{content:\"\\231B\"}.glyphicon-lamp:before{content:\"\\E223\"}.glyphicon-duplicate:before{content:\"\\E224\"}.glyphicon-piggy-bank:before{content:\"\\E225\"}.glyphicon-scissors:before{content:\"\\E226\"}.glyphicon-bitcoin:before{content:\"\\E227\"}.glyphicon-btc:before{content:\"\\E227\"}.glyphicon-xbt:before{content:\"\\E227\"}.glyphicon-yen:before{content:\"\\A5\"}.glyphicon-jpy:before{content:\"\\A5\"}.glyphicon-ruble:before{content:\"\\20BD\"}.glyphicon-rub:before{content:\"\\20BD\"}.glyphicon-scale:before{content:\"\\E230\"}.glyphicon-ice-lolly:before{content:\"\\E231\"}.glyphicon-ice-lolly-tasted:before{content:\"\\E232\"}.glyphicon-education:before{content:\"\\E233\"}.glyphicon-option-horizontal:before{content:\"\\E234\"}.glyphicon-option-vertical:before{content:\"\\E235\"}.glyphicon-menu-hamburger:before{content:\"\\E236\"}.glyphicon-modal-window:before{content:\"\\E237\"}.glyphicon-oil:before{content:\"\\E238\"}.glyphicon-grain:before{content:\"\\E239\"}.glyphicon-sunglasses:before{content:\"\\E240\"}.glyphicon-text-size:before{content:\"\\E241\"}.glyphicon-text-color:before{content:\"\\E242\"}.glyphicon-text-background:before{content:\"\\E243\"}.glyphicon-object-align-top:before{content:\"\\E244\"}.glyphicon-object-align-bottom:before{content:\"\\E245\"}.glyphicon-object-align-horizontal:before{content:\"\\E246\"}.glyphicon-object-align-left:before{content:\"\\E247\"}.glyphicon-object-align-vertical:before{content:\"\\E248\"}.glyphicon-object-align-right:before{content:\"\\E249\"}.glyphicon-triangle-right:before{content:\"\\E250\"}.glyphicon-triangle-left:before{content:\"\\E251\"}.glyphicon-triangle-bottom:before{content:\"\\E252\"}.glyphicon-triangle-top:before{content:\"\\E253\"}.glyphicon-console:before{content:\"\\E254\"}.glyphicon-superscript:before{content:\"\\E255\"}.glyphicon-subscript:before{content:\"\\E256\"}.glyphicon-menu-left:before{content:\"\\E257\"}.glyphicon-menu-right:before{content:\"\\E258\"}.glyphicon-menu-down:before{content:\"\\E259\"}.glyphicon-menu-up:before{content:\"\\E260\"}*{-webkit-box-sizing:border-box;-moz-box-sizing:border-box;box-sizing:border-box}:after,:before{-webkit-box-sizing:border-box;-moz-box-sizing:border-box;box-sizing:border-box}html{font-size:10px;-webkit-tap-highlight-color:rgba(0,0,0,0)}body{font-family:\"Helvetica Neue\",Helvetica,Arial,sans-serif;font-size:14px;line-height:1.42857143;color:#333;background-color:#fff}button,input,select,textarea{font-family:inherit;font-size:inherit;line-height:inherit}a{color:#337ab7;text-decoration:none}a:focus,a:hover{color:#23527c;text-decoration:underline}a:focus{outline:thin dotted;outline:5px auto -webkit-focus-ring-color;outline-offset:-2px}figure{margin:0}img{vertical-align:middle}.carousel-inner>.item>a>img,.carousel-inner>.item>img,.img-responsive,.thumbnail a>img,.thumbnail>img{display:block;max-width:100%;height:auto}.img-rounded{border-radius:6px}.img-thumbnail{display:inline-block;max-width:100%;height:auto;padding:4px;line-height:1.42857143;background-color:#fff;border:1px solid #ddd;border-radius:4px;-webkit-transition:all .2s ease-in-out;-o-transition:all .2s ease-in-out;transition:all .2s ease-in-out}.img-circle{border-radius:50%}hr{margin-top:20px;margin-bottom:20px;border:0;border-top:1px solid #eee}.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);border:0}.sr-only-focusable:active,.sr-only-focusable:focus{position:static;width:auto;height:auto;margin:0;overflow:visible;clip:auto}[role=button]{cursor:pointer}.h1,.h2,.h3,.h4,.h5,.h6,h1,h2,h3,h4,h5,h6{font-family:inherit;font-weight:500;line-height:1.1;color:inherit}.h1 .small,.h1 small,.h2 .small,.h2 small,.h3 .small,.h3 small,.h4 .small,.h4 small,.h5 .small,.h5 small,.h6 .small,.h6 small,h1 .small,h1 small,h2 .small,h2 small,h3 .small,h3 small,h4 .small,h4 small,h5 .small,h5 small,h6 .small,h6 small{font-weight:400;line-height:1;color:#777}.h1,.h2,.h3,h1,h2,h3{margin-top:20px;margin-bottom:10px}.h1 .small,.h1 small,.h2 .small,.h2 small,.h3 .small,.h3 small,h1 .small,h1 small,h2 .small,h2 small,h3 .small,h3 small{font-size:65%}.h4,.h5,.h6,h4,h5,h6{margin-top:10px;margin-bottom:10px}.h4 .small,.h4 small,.h5 .small,.h5 small,.h6 .small,.h6 small,h4 .small,h4 small,h5 .small,h5 small,h6 .small,h6 small{font-size:75%}.h1,h1{font-size:36px}.h2,h2{font-size:30px}.h3,h3{font-size:24px}.h4,h4{font-size:18px}.h5,h5{font-size:14px}.h6,h6{font-size:12px}p{margin:0 0 10px}.lead{margin-bottom:20px;font-size:16px;font-weight:300;line-height:1.4}@media (min-width:768px){.lead{font-size:21px}}.small,small{font-size:85%}.mark,mark{padding:.2em;background-color:#fcf8e3}.text-left{text-align:left}.text-right{text-align:right}.text-center{text-align:center}.text-justify{text-align:justify}.text-nowrap{white-space:nowrap}.text-lowercase{text-transform:lowercase}.text-uppercase{text-transform:uppercase}.text-capitalize{text-transform:capitalize}.text-muted{color:#777}.text-primary{color:#337ab7}a.text-primary:focus,a.text-primary:hover{color:#286090}.text-success{color:#3c763d}a.text-success:focus,a.text-success:hover{color:#2b542c}.text-info{color:#31708f}a.text-info:focus,a.text-info:hover{color:#245269}.text-warning{color:#8a6d3b}a.text-warning:focus,a.text-warning:hover{color:#66512c}.text-danger{color:#a94442}a.text-danger:focus,a.text-danger:hover{color:#843534}.bg-primary{color:#fff;background-color:#337ab7}a.bg-primary:focus,a.bg-primary:hover{background-color:#286090}.bg-success{background-color:#dff0d8}a.bg-success:focus,a.bg-success:hover{background-color:#c1e2b3}.bg-info{background-color:#d9edf7}a.bg-info:focus,a.bg-info:hover{background-color:#afd9ee}.bg-warning{background-color:#fcf8e3}a.bg-warning:focus,a.bg-warning:hover{background-color:#f7ecb5}.bg-danger{background-color:#f2dede}a.bg-danger:focus,a.bg-danger:hover{background-color:#e4b9b9}.page-header{padding-bottom:9px;margin:40px 0 20px;border-bottom:1px solid #eee}ol,ul{margin-top:0;margin-bottom:10px}ol ol,ol ul,ul ol,ul ul{margin-bottom:0}.list-unstyled{padding-left:0;list-style:none}.list-inline{padding-left:0;margin-left:-5px;list-style:none}.list-inline>li{display:inline-block;padding-right:5px;padding-left:5px}dl{margin-top:0;margin-bottom:20px}dd,dt{line-height:1.42857143}dt{font-weight:700}dd{margin-left:0}@media (min-width:768px){.dl-horizontal dt{float:left;width:160px;overflow:hidden;clear:left;text-align:right;text-overflow:ellipsis;white-space:nowrap}.dl-horizontal dd{margin-left:180px}}abbr[data-original-title],abbr[title]{cursor:help;border-bottom:1px dotted #777}.initialism{font-size:90%;text-transform:uppercase}blockquote{padding:10px 20px;margin:0 0 20px;font-size:17.5px;border-left:5px solid #eee}blockquote ol:last-child,blockquote p:last-child,blockquote ul:last-child{margin-bottom:0}blockquote .small,blockquote footer,blockquote small{display:block;font-size:80%;line-height:1.42857143;color:#777}blockquote .small:before,blockquote footer:before,blockquote small:before{content:'\\2014   \\A0'}.blockquote-reverse,blockquote.pull-right{padding-right:15px;padding-left:0;text-align:right;border-right:5px solid #eee;border-left:0}.blockquote-reverse .small:before,.blockquote-reverse footer:before,.blockquote-reverse small:before,blockquote.pull-right .small:before,blockquote.pull-right footer:before,blockquote.pull-right small:before{content:''}.blockquote-reverse .small:after,.blockquote-reverse footer:after,.blockquote-reverse small:after,blockquote.pull-right .small:after,blockquote.pull-right footer:after,blockquote.pull-right small:after{content:'\\A0   \\2014'}address{margin-bottom:20px;font-style:normal;line-height:1.42857143}code,kbd,pre,samp{font-family:Menlo,Monaco,Consolas,\"Courier New\",monospace}code{padding:2px 4px;font-size:90%;color:#c7254e;background-color:#f9f2f4;border-radius:4px}kbd{padding:2px 4px;font-size:90%;color:#fff;background-color:#333;border-radius:3px;-webkit-box-shadow:inset 0 -1px 0 rgba(0,0,0,.25);box-shadow:inset 0 -1px 0 rgba(0,0,0,.25)}kbd kbd{padding:0;font-size:100%;font-weight:700;-webkit-box-shadow:none;box-shadow:none}pre{display:block;padding:9.5px;margin:0 0 10px;font-size:13px;line-height:1.42857143;color:#333;word-break:break-all;word-wrap:break-word;background-color:#f5f5f5;border:1px solid #ccc;border-radius:4px}pre code{padding:0;font-size:inherit;color:inherit;white-space:pre-wrap;background-color:transparent;border-radius:0}.pre-scrollable{max-height:340px;overflow-y:scroll}.container{padding-right:15px;padding-left:15px;margin-right:auto;margin-left:auto}@media (min-width:768px){.container{width:750px}}@media (min-width:992px){.container{width:970px}}@media (min-width:1200px){.container{width:1170px}}.container-fluid{padding-right:15px;padding-left:15px;margin-right:auto;margin-left:auto}.row{margin-right:-15px;margin-left:-15px}.col-lg-1,.col-lg-10,.col-lg-11,.col-lg-12,.col-lg-2,.col-lg-3,.col-lg-4,.col-lg-5,.col-lg-6,.col-lg-7,.col-lg-8,.col-lg-9,.col-md-1,.col-md-10,.col-md-11,.col-md-12,.col-md-2,.col-md-3,.col-md-4,.col-md-5,.col-md-6,.col-md-7,.col-md-8,.col-md-9,.col-sm-1,.col-sm-10,.col-sm-11,.col-sm-12,.col-sm-2,.col-sm-3,.col-sm-4,.col-sm-5,.col-sm-6,.col-sm-7,.col-sm-8,.col-sm-9,.col-xs-1,.col-xs-10,.col-xs-11,.col-xs-12,.col-xs-2,.col-xs-3,.col-xs-4,.col-xs-5,.col-xs-6,.col-xs-7,.col-xs-8,.col-xs-9{position:relative;min-height:1px;padding-right:15px;padding-left:15px}.col-xs-1,.col-xs-10,.col-xs-11,.col-xs-12,.col-xs-2,.col-xs-3,.col-xs-4,.col-xs-5,.col-xs-6,.col-xs-7,.col-xs-8,.col-xs-9{float:left}.col-xs-12{width:100%}.col-xs-11{width:91.66666667%}.col-xs-10{width:83.33333333%}.col-xs-9{width:75%}.col-xs-8{width:66.66666667%}.col-xs-7{width:58.33333333%}.col-xs-6{width:50%}.col-xs-5{width:41.66666667%}.col-xs-4{width:33.33333333%}.col-xs-3{width:25%}.col-xs-2{width:16.66666667%}.col-xs-1{width:8.33333333%}.col-xs-pull-12{right:100%}.col-xs-pull-11{right:91.66666667%}.col-xs-pull-10{right:83.33333333%}.col-xs-pull-9{right:75%}.col-xs-pull-8{right:66.66666667%}.col-xs-pull-7{right:58.33333333%}.col-xs-pull-6{right:50%}.col-xs-pull-5{right:41.66666667%}.col-xs-pull-4{right:33.33333333%}.col-xs-pull-3{right:25%}.col-xs-pull-2{right:16.66666667%}.col-xs-pull-1{right:8.33333333%}.col-xs-pull-0{right:auto}.col-xs-push-12{left:100%}.col-xs-push-11{left:91.66666667%}.col-xs-push-10{left:83.33333333%}.col-xs-push-9{left:75%}.col-xs-push-8{left:66.66666667%}.col-xs-push-7{left:58.33333333%}.col-xs-push-6{left:50%}.col-xs-push-5{left:41.66666667%}.col-xs-push-4{left:33.33333333%}.col-xs-push-3{left:25%}.col-xs-push-2{left:16.66666667%}.col-xs-push-1{left:8.33333333%}.col-xs-push-0{left:auto}.col-xs-offset-12{margin-left:100%}.col-xs-offset-11{margin-left:91.66666667%}.col-xs-offset-10{margin-left:83.33333333%}.col-xs-offset-9{margin-left:75%}.col-xs-offset-8{margin-left:66.66666667%}.col-xs-offset-7{margin-left:58.33333333%}.col-xs-offset-6{margin-left:50%}.col-xs-offset-5{margin-left:41.66666667%}.col-xs-offset-4{margin-left:33.33333333%}.col-xs-offset-3{margin-left:25%}.col-xs-offset-2{margin-left:16.66666667%}.col-xs-offset-1{margin-left:8.33333333%}.col-xs-offset-0{margin-left:0}@media (min-width:768px){.col-sm-1,.col-sm-10,.col-sm-11,.col-sm-12,.col-sm-2,.col-sm-3,.col-sm-4,.col-sm-5,.col-sm-6,.col-sm-7,.col-sm-8,.col-sm-9{float:left}.col-sm-12{width:100%}.col-sm-11{width:91.66666667%}.col-sm-10{width:83.33333333%}.col-sm-9{width:75%}.col-sm-8{width:66.66666667%}.col-sm-7{width:58.33333333%}.col-sm-6{width:50%}.col-sm-5{width:41.66666667%}.col-sm-4{width:33.33333333%}.col-sm-3{width:25%}.col-sm-2{width:16.66666667%}.col-sm-1{width:8.33333333%}.col-sm-pull-12{right:100%}.col-sm-pull-11{right:91.66666667%}.col-sm-pull-10{right:83.33333333%}.col-sm-pull-9{right:75%}.col-sm-pull-8{right:66.66666667%}.col-sm-pull-7{right:58.33333333%}.col-sm-pull-6{right:50%}.col-sm-pull-5{right:41.66666667%}.col-sm-pull-4{right:33.33333333%}.col-sm-pull-3{right:25%}.col-sm-pull-2{right:16.66666667%}.col-sm-pull-1{right:8.33333333%}.col-sm-pull-0{right:auto}.col-sm-push-12{left:100%}.col-sm-push-11{left:91.66666667%}.col-sm-push-10{left:83.33333333%}.col-sm-push-9{left:75%}.col-sm-push-8{left:66.66666667%}.col-sm-push-7{left:58.33333333%}.col-sm-push-6{left:50%}.col-sm-push-5{left:41.66666667%}.col-sm-push-4{left:33.33333333%}.col-sm-push-3{left:25%}.col-sm-push-2{left:16.66666667%}.col-sm-push-1{left:8.33333333%}.col-sm-push-0{left:auto}.col-sm-offset-12{margin-left:100%}.col-sm-offset-11{margin-left:91.66666667%}.col-sm-offset-10{margin-left:83.33333333%}.col-sm-offset-9{margin-left:75%}.col-sm-offset-8{margin-left:66.66666667%}.col-sm-offset-7{margin-left:58.33333333%}.col-sm-offset-6{margin-left:50%}.col-sm-offset-5{margin-left:41.66666667%}.col-sm-offset-4{margin-left:33.33333333%}.col-sm-offset-3{margin-left:25%}.col-sm-offset-2{margin-left:16.66666667%}.col-sm-offset-1{margin-left:8.33333333%}.col-sm-offset-0{margin-left:0}}@media (min-width:992px){.col-md-1,.col-md-10,.col-md-11,.col-md-12,.col-md-2,.col-md-3,.col-md-4,.col-md-5,.col-md-6,.col-md-7,.col-md-8,.col-md-9{float:left}.col-md-12{width:100%}.col-md-11{width:91.66666667%}.col-md-10{width:83.33333333%}.col-md-9{width:75%}.col-md-8{width:66.66666667%}.col-md-7{width:58.33333333%}.col-md-6{width:50%}.col-md-5{width:41.66666667%}.col-md-4{width:33.33333333%}.col-md-3{width:25%}.col-md-2{width:16.66666667%}.col-md-1{width:8.33333333%}.col-md-pull-12{right:100%}.col-md-pull-11{right:91.66666667%}.col-md-pull-10{right:83.33333333%}.col-md-pull-9{right:75%}.col-md-pull-8{right:66.66666667%}.col-md-pull-7{right:58.33333333%}.col-md-pull-6{right:50%}.col-md-pull-5{right:41.66666667%}.col-md-pull-4{right:33.33333333%}.col-md-pull-3{right:25%}.col-md-pull-2{right:16.66666667%}.col-md-pull-1{right:8.33333333%}.col-md-pull-0{right:auto}.col-md-push-12{left:100%}.col-md-push-11{left:91.66666667%}.col-md-push-10{left:83.33333333%}.col-md-push-9{left:75%}.col-md-push-8{left:66.66666667%}.col-md-push-7{left:58.33333333%}.col-md-push-6{left:50%}.col-md-push-5{left:41.66666667%}.col-md-push-4{left:33.33333333%}.col-md-push-3{left:25%}.col-md-push-2{left:16.66666667%}.col-md-push-1{left:8.33333333%}.col-md-push-0{left:auto}.col-md-offset-12{margin-left:100%}.col-md-offset-11{margin-left:91.66666667%}.col-md-offset-10{margin-left:83.33333333%}.col-md-offset-9{margin-left:75%}.col-md-offset-8{margin-left:66.66666667%}.col-md-offset-7{margin-left:58.33333333%}.col-md-offset-6{margin-left:50%}.col-md-offset-5{margin-left:41.66666667%}.col-md-offset-4{margin-left:33.33333333%}.col-md-offset-3{margin-left:25%}.col-md-offset-2{margin-left:16.66666667%}.col-md-offset-1{margin-left:8.33333333%}.col-md-offset-0{margin-left:0}}@media (min-width:1200px){.col-lg-1,.col-lg-10,.col-lg-11,.col-lg-12,.col-lg-2,.col-lg-3,.col-lg-4,.col-lg-5,.col-lg-6,.col-lg-7,.col-lg-8,.col-lg-9{float:left}.col-lg-12{width:100%}.col-lg-11{width:91.66666667%}.col-lg-10{width:83.33333333%}.col-lg-9{width:75%}.col-lg-8{width:66.66666667%}.col-lg-7{width:58.33333333%}.col-lg-6{width:50%}.col-lg-5{width:41.66666667%}.col-lg-4{width:33.33333333%}.col-lg-3{width:25%}.col-lg-2{width:16.66666667%}.col-lg-1{width:8.33333333%}.col-lg-pull-12{right:100%}.col-lg-pull-11{right:91.66666667%}.col-lg-pull-10{right:83.33333333%}.col-lg-pull-9{right:75%}.col-lg-pull-8{right:66.66666667%}.col-lg-pull-7{right:58.33333333%}.col-lg-pull-6{right:50%}.col-lg-pull-5{right:41.66666667%}.col-lg-pull-4{right:33.33333333%}.col-lg-pull-3{right:25%}.col-lg-pull-2{right:16.66666667%}.col-lg-pull-1{right:8.33333333%}.col-lg-pull-0{right:auto}.col-lg-push-12{left:100%}.col-lg-push-11{left:91.66666667%}.col-lg-push-10{left:83.33333333%}.col-lg-push-9{left:75%}.col-lg-push-8{left:66.66666667%}.col-lg-push-7{left:58.33333333%}.col-lg-push-6{left:50%}.col-lg-push-5{left:41.66666667%}.col-lg-push-4{left:33.33333333%}.col-lg-push-3{left:25%}.col-lg-push-2{left:16.66666667%}.col-lg-push-1{left:8.33333333%}.col-lg-push-0{left:auto}.col-lg-offset-12{margin-left:100%}.col-lg-offset-11{margin-left:91.66666667%}.col-lg-offset-10{margin-left:83.33333333%}.col-lg-offset-9{margin-left:75%}.col-lg-offset-8{margin-left:66.66666667%}.col-lg-offset-7{margin-left:58.33333333%}.col-lg-offset-6{margin-left:50%}.col-lg-offset-5{margin-left:41.66666667%}.col-lg-offset-4{margin-left:33.33333333%}.col-lg-offset-3{margin-left:25%}.col-lg-offset-2{margin-left:16.66666667%}.col-lg-offset-1{margin-left:8.33333333%}.col-lg-offset-0{margin-left:0}}table{background-color:transparent}caption{padding-top:8px;padding-bottom:8px;color:#777;text-align:left}th{text-align:left}.table{width:100%;max-width:100%;margin-bottom:20px}.table>tbody>tr>td,.table>tbody>tr>th,.table>tfoot>tr>td,.table>tfoot>tr>th,.table>thead>tr>td,.table>thead>tr>th{padding:8px;line-height:1.42857143;vertical-align:top;border-top:1px solid #ddd}.table>thead>tr>th{vertical-align:bottom;border-bottom:2px solid #ddd}.table>caption+thead>tr:first-child>td,.table>caption+thead>tr:first-child>th,.table>colgroup+thead>tr:first-child>td,.table>colgroup+thead>tr:first-child>th,.table>thead:first-child>tr:first-child>td,.table>thead:first-child>tr:first-child>th{border-top:0}.table>tbody+tbody{border-top:2px solid #ddd}.table .table{background-color:#fff}.table-condensed>tbody>tr>td,.table-condensed>tbody>tr>th,.table-condensed>tfoot>tr>td,.table-condensed>tfoot>tr>th,.table-condensed>thead>tr>td,.table-condensed>thead>tr>th{padding:5px}.table-bordered{border:1px solid #ddd}.table-bordered>tbody>tr>td,.table-bordered>tbody>tr>th,.table-bordered>tfoot>tr>td,.table-bordered>tfoot>tr>th,.table-bordered>thead>tr>td,.table-bordered>thead>tr>th{border:1px solid #ddd}.table-bordered>thead>tr>td,.table-bordered>thead>tr>th{border-bottom-width:2px}.table-striped>tbody>tr:nth-of-type(odd){background-color:#f9f9f9}.table-hover>tbody>tr:hover{background-color:#f5f5f5}table col[class*=col-]{position:static;display:table-column;float:none}table td[class*=col-],table th[class*=col-]{position:static;display:table-cell;float:none}.table>tbody>tr.active>td,.table>tbody>tr.active>th,.table>tbody>tr>td.active,.table>tbody>tr>th.active,.table>tfoot>tr.active>td,.table>tfoot>tr.active>th,.table>tfoot>tr>td.active,.table>tfoot>tr>th.active,.table>thead>tr.active>td,.table>thead>tr.active>th,.table>thead>tr>td.active,.table>thead>tr>th.active{background-color:#f5f5f5}.table-hover>tbody>tr.active:hover>td,.table-hover>tbody>tr.active:hover>th,.table-hover>tbody>tr:hover>.active,.table-hover>tbody>tr>td.active:hover,.table-hover>tbody>tr>th.active:hover{background-color:#e8e8e8}.table>tbody>tr.success>td,.table>tbody>tr.success>th,.table>tbody>tr>td.success,.table>tbody>tr>th.success,.table>tfoot>tr.success>td,.table>tfoot>tr.success>th,.table>tfoot>tr>td.success,.table>tfoot>tr>th.success,.table>thead>tr.success>td,.table>thead>tr.success>th,.table>thead>tr>td.success,.table>thead>tr>th.success{background-color:#dff0d8}.table-hover>tbody>tr.success:hover>td,.table-hover>tbody>tr.success:hover>th,.table-hover>tbody>tr:hover>.success,.table-hover>tbody>tr>td.success:hover,.table-hover>tbody>tr>th.success:hover{background-color:#d0e9c6}.table>tbody>tr.info>td,.table>tbody>tr.info>th,.table>tbody>tr>td.info,.table>tbody>tr>th.info,.table>tfoot>tr.info>td,.table>tfoot>tr.info>th,.table>tfoot>tr>td.info,.table>tfoot>tr>th.info,.table>thead>tr.info>td,.table>thead>tr.info>th,.table>thead>tr>td.info,.table>thead>tr>th.info{background-color:#d9edf7}.table-hover>tbody>tr.info:hover>td,.table-hover>tbody>tr.info:hover>th,.table-hover>tbody>tr:hover>.info,.table-hover>tbody>tr>td.info:hover,.table-hover>tbody>tr>th.info:hover{background-color:#c4e3f3}.table>tbody>tr.warning>td,.table>tbody>tr.warning>th,.table>tbody>tr>td.warning,.table>tbody>tr>th.warning,.table>tfoot>tr.warning>td,.table>tfoot>tr.warning>th,.table>tfoot>tr>td.warning,.table>tfoot>tr>th.warning,.table>thead>tr.warning>td,.table>thead>tr.warning>th,.table>thead>tr>td.warning,.table>thead>tr>th.warning{background-color:#fcf8e3}.table-hover>tbody>tr.warning:hover>td,.table-hover>tbody>tr.warning:hover>th,.table-hover>tbody>tr:hover>.warning,.table-hover>tbody>tr>td.warning:hover,.table-hover>tbody>tr>th.warning:hover{background-color:#faf2cc}.table>tbody>tr.danger>td,.table>tbody>tr.danger>th,.table>tbody>tr>td.danger,.table>tbody>tr>th.danger,.table>tfoot>tr.danger>td,.table>tfoot>tr.danger>th,.table>tfoot>tr>td.danger,.table>tfoot>tr>th.danger,.table>thead>tr.danger>td,.table>thead>tr.danger>th,.table>thead>tr>td.danger,.table>thead>tr>th.danger{background-color:#f2dede}.table-hover>tbody>tr.danger:hover>td,.table-hover>tbody>tr.danger:hover>th,.table-hover>tbody>tr:hover>.danger,.table-hover>tbody>tr>td.danger:hover,.table-hover>tbody>tr>th.danger:hover{background-color:#ebcccc}.table-responsive{min-height:.01%;overflow-x:auto}@media screen and (max-width:767px){.table-responsive{width:100%;margin-bottom:15px;overflow-y:hidden;-ms-overflow-style:-ms-autohiding-scrollbar;border:1px solid #ddd}.table-responsive>.table{margin-bottom:0}.table-responsive>.table>tbody>tr>td,.table-responsive>.table>tbody>tr>th,.table-responsive>.table>tfoot>tr>td,.table-responsive>.table>tfoot>tr>th,.table-responsive>.table>thead>tr>td,.table-responsive>.table>thead>tr>th{white-space:nowrap}.table-responsive>.table-bordered{border:0}.table-responsive>.table-bordered>tbody>tr>td:first-child,.table-responsive>.table-bordered>tbody>tr>th:first-child,.table-responsive>.table-bordered>tfoot>tr>td:first-child,.table-responsive>.table-bordered>tfoot>tr>th:first-child,.table-responsive>.table-bordered>thead>tr>td:first-child,.table-responsive>.table-bordered>thead>tr>th:first-child{border-left:0}.table-responsive>.table-bordered>tbody>tr>td:last-child,.table-responsive>.table-bordered>tbody>tr>th:last-child,.table-responsive>.table-bordered>tfoot>tr>td:last-child,.table-responsive>.table-bordered>tfoot>tr>th:last-child,.table-responsive>.table-bordered>thead>tr>td:last-child,.table-responsive>.table-bordered>thead>tr>th:last-child{border-right:0}.table-responsive>.table-bordered>tbody>tr:last-child>td,.table-responsive>.table-bordered>tbody>tr:last-child>th,.table-responsive>.table-bordered>tfoot>tr:last-child>td,.table-responsive>.table-bordered>tfoot>tr:last-child>th{border-bottom:0}}fieldset{min-width:0;padding:0;margin:0;border:0}legend{display:block;width:100%;padding:0;margin-bottom:20px;font-size:21px;line-height:inherit;color:#333;border:0;border-bottom:1px solid #e5e5e5}label{display:inline-block;max-width:100%;margin-bottom:5px;font-weight:700}input[type=search]{-webkit-box-sizing:border-box;-moz-box-sizing:border-box;box-sizing:border-box}input[type=checkbox],input[type=radio]{margin:4px 0 0;margin-top:1px\\9;line-height:normal}input[type=file]{display:block}input[type=range]{display:block;width:100%}select[multiple],select[size]{height:auto}input[type=file]:focus,input[type=checkbox]:focus,input[type=radio]:focus{outline:thin dotted;outline:5px auto -webkit-focus-ring-color;outline-offset:-2px}output{display:block;padding-top:7px;font-size:14px;line-height:1.42857143;color:#555}.form-control{display:block;width:100%;height:34px;padding:6px 12px;font-size:14px;line-height:1.42857143;color:#555;background-color:#fff;background-image:none;border:1px solid #ccc;border-radius:4px;-webkit-box-shadow:inset 0 1px 1px rgba(0,0,0,.075);box-shadow:inset 0 1px 1px rgba(0,0,0,.075);-webkit-transition:border-color ease-in-out .15s,-webkit-box-shadow ease-in-out .15s;-o-transition:border-color ease-in-out .15s,box-shadow ease-in-out .15s;transition:border-color ease-in-out .15s,box-shadow ease-in-out .15s}.form-control:focus{border-color:#66afe9;outline:0;-webkit-box-shadow:inset 0 1px 1px rgba(0,0,0,.075),0 0 8px rgba(102,175,233,.6);box-shadow:inset 0 1px 1px rgba(0,0,0,.075),0 0 8px rgba(102,175,233,.6)}.form-control::-moz-placeholder{color:#999;opacity:1}.form-control:-ms-input-placeholder{color:#999}.form-control::-webkit-input-placeholder{color:#999}.form-control::-ms-expand{background-color:transparent;border:0}.form-control[disabled],.form-control[readonly],fieldset[disabled] .form-control{background-color:#eee;opacity:1}.form-control[disabled],fieldset[disabled] .form-control{cursor:not-allowed}textarea.form-control{height:auto}input[type=search]{-webkit-appearance:none}@media screen and (-webkit-min-device-pixel-ratio:0){input[type=date].form-control,input[type=time].form-control,input[type=datetime-local].form-control,input[type=month].form-control{line-height:34px}.input-group-sm input[type=date],.input-group-sm input[type=time],.input-group-sm input[type=datetime-local],.input-group-sm input[type=month],input[type=date].input-sm,input[type=time].input-sm,input[type=datetime-local].input-sm,input[type=month].input-sm{line-height:30px}.input-group-lg input[type=date],.input-group-lg input[type=time],.input-group-lg input[type=datetime-local],.input-group-lg input[type=month],input[type=date].input-lg,input[type=time].input-lg,input[type=datetime-local].input-lg,input[type=month].input-lg{line-height:46px}}.form-group{margin-bottom:15px}.checkbox,.radio{position:relative;display:block;margin-top:10px;margin-bottom:10px}.checkbox label,.radio label{min-height:20px;padding-left:20px;margin-bottom:0;font-weight:400;cursor:pointer}.checkbox input[type=checkbox],.checkbox-inline input[type=checkbox],.radio input[type=radio],.radio-inline input[type=radio]{position:absolute;margin-top:4px\\9;margin-left:-20px}.checkbox+.checkbox,.radio+.radio{margin-top:-5px}.checkbox-inline,.radio-inline{position:relative;display:inline-block;padding-left:20px;margin-bottom:0;font-weight:400;vertical-align:middle;cursor:pointer}.checkbox-inline+.checkbox-inline,.radio-inline+.radio-inline{margin-top:0;margin-left:10px}fieldset[disabled] input[type=checkbox],fieldset[disabled] input[type=radio],input[type=checkbox].disabled,input[type=checkbox][disabled],input[type=radio].disabled,input[type=radio][disabled]{cursor:not-allowed}.checkbox-inline.disabled,.radio-inline.disabled,fieldset[disabled] .checkbox-inline,fieldset[disabled] .radio-inline{cursor:not-allowed}.checkbox.disabled label,.radio.disabled label,fieldset[disabled] .checkbox label,fieldset[disabled] .radio label{cursor:not-allowed}.form-control-static{min-height:34px;padding-top:7px;padding-bottom:7px;margin-bottom:0}.form-control-static.input-lg,.form-control-static.input-sm{padding-right:0;padding-left:0}.input-sm{height:30px;padding:5px 10px;font-size:12px;line-height:1.5;border-radius:3px}select.input-sm{height:30px;line-height:30px}select[multiple].input-sm,textarea.input-sm{height:auto}.form-group-sm .form-control{height:30px;padding:5px 10px;font-size:12px;line-height:1.5;border-radius:3px}.form-group-sm select.form-control{height:30px;line-height:30px}.form-group-sm select[multiple].form-control,.form-group-sm textarea.form-control{height:auto}.form-group-sm .form-control-static{height:30px;min-height:32px;padding:6px 10px;font-size:12px;line-height:1.5}.input-lg{height:46px;padding:10px 16px;font-size:18px;line-height:1.3333333;border-radius:6px}select.input-lg{height:46px;line-height:46px}select[multiple].input-lg,textarea.input-lg{height:auto}.form-group-lg .form-control{height:46px;padding:10px 16px;font-size:18px;line-height:1.3333333;border-radius:6px}.form-group-lg select.form-control{height:46px;line-height:46px}.form-group-lg select[multiple].form-control,.form-group-lg textarea.form-control{height:auto}.form-group-lg .form-control-static{height:46px;min-height:38px;padding:11px 16px;font-size:18px;line-height:1.3333333}.has-feedback{position:relative}.has-feedback .form-control{padding-right:42.5px}.form-control-feedback{position:absolute;top:0;right:0;z-index:2;display:block;width:34px;height:34px;line-height:34px;text-align:center;pointer-events:none}.form-group-lg .form-control+.form-control-feedback,.input-group-lg+.form-control-feedback,.input-lg+.form-control-feedback{width:46px;height:46px;line-height:46px}.form-group-sm .form-control+.form-control-feedback,.input-group-sm+.form-control-feedback,.input-sm+.form-control-feedback{width:30px;height:30px;line-height:30px}.has-success .checkbox,.has-success .checkbox-inline,.has-success .control-label,.has-success .help-block,.has-success .radio,.has-success .radio-inline,.has-success.checkbox label,.has-success.checkbox-inline label,.has-success.radio label,.has-success.radio-inline label{color:#3c763d}.has-success .form-control{border-color:#3c763d;-webkit-box-shadow:inset 0 1px 1px rgba(0,0,0,.075);box-shadow:inset 0 1px 1px rgba(0,0,0,.075)}.has-success .form-control:focus{border-color:#2b542c;-webkit-box-shadow:inset 0 1px 1px rgba(0,0,0,.075),0 0 6px #67b168;box-shadow:inset 0 1px 1px rgba(0,0,0,.075),0 0 6px #67b168}.has-success .input-group-addon{color:#3c763d;background-color:#dff0d8;border-color:#3c763d}.has-success .form-control-feedback{color:#3c763d}.has-warning .checkbox,.has-warning .checkbox-inline,.has-warning .control-label,.has-warning .help-block,.has-warning .radio,.has-warning .radio-inline,.has-warning.checkbox label,.has-warning.checkbox-inline label,.has-warning.radio label,.has-warning.radio-inline label{color:#8a6d3b}.has-warning .form-control{border-color:#8a6d3b;-webkit-box-shadow:inset 0 1px 1px rgba(0,0,0,.075);box-shadow:inset 0 1px 1px rgba(0,0,0,.075)}.has-warning .form-control:focus{border-color:#66512c;-webkit-box-shadow:inset 0 1px 1px rgba(0,0,0,.075),0 0 6px #c0a16b;box-shadow:inset 0 1px 1px rgba(0,0,0,.075),0 0 6px #c0a16b}.has-warning .input-group-addon{color:#8a6d3b;background-color:#fcf8e3;border-color:#8a6d3b}.has-warning .form-control-feedback{color:#8a6d3b}.has-error .checkbox,.has-error .checkbox-inline,.has-error .control-label,.has-error .help-block,.has-error .radio,.has-error .radio-inline,.has-error.checkbox label,.has-error.checkbox-inline label,.has-error.radio label,.has-error.radio-inline label{color:#a94442}.has-error .form-control{border-color:#a94442;-webkit-box-shadow:inset 0 1px 1px rgba(0,0,0,.075);box-shadow:inset 0 1px 1px rgba(0,0,0,.075)}.has-error .form-control:focus{border-color:#843534;-webkit-box-shadow:inset 0 1px 1px rgba(0,0,0,.075),0 0 6px #ce8483;box-shadow:inset 0 1px 1px rgba(0,0,0,.075),0 0 6px #ce8483}.has-error .input-group-addon{color:#a94442;background-color:#f2dede;border-color:#a94442}.has-error .form-control-feedback{color:#a94442}.has-feedback label~.form-control-feedback{top:25px}.has-feedback label.sr-only~.form-control-feedback{top:0}.help-block{display:block;margin-top:5px;margin-bottom:10px;color:#737373}@media (min-width:768px){.form-inline .form-group{display:inline-block;margin-bottom:0;vertical-align:middle}.form-inline .form-control{display:inline-block;width:auto;vertical-align:middle}.form-inline .form-control-static{display:inline-block}.form-inline .input-group{display:inline-table;vertical-align:middle}.form-inline .input-group .form-control,.form-inline .input-group .input-group-addon,.form-inline .input-group .input-group-btn{width:auto}.form-inline .input-group>.form-control{width:100%}.form-inline .control-label{margin-bottom:0;vertical-align:middle}.form-inline .checkbox,.form-inline .radio{display:inline-block;margin-top:0;margin-bottom:0;vertical-align:middle}.form-inline .checkbox label,.form-inline .radio label{padding-left:0}.form-inline .checkbox input[type=checkbox],.form-inline .radio input[type=radio]{position:relative;margin-left:0}.form-inline .has-feedback .form-control-feedback{top:0}}.form-horizontal .checkbox,.form-horizontal .checkbox-inline,.form-horizontal .radio,.form-horizontal .radio-inline{padding-top:7px;margin-top:0;margin-bottom:0}.form-horizontal .checkbox,.form-horizontal .radio{min-height:27px}.form-horizontal .form-group{margin-right:-15px;margin-left:-15px}@media (min-width:768px){.form-horizontal .control-label{padding-top:7px;margin-bottom:0;text-align:right}}.form-horizontal .has-feedback .form-control-feedback{right:15px}@media (min-width:768px){.form-horizontal .form-group-lg .control-label{padding-top:11px;font-size:18px}}@media (min-width:768px){.form-horizontal .form-group-sm .control-label{padding-top:6px;font-size:12px}}.btn{display:inline-block;padding:6px 12px;margin-bottom:0;font-size:14px;font-weight:400;line-height:1.42857143;text-align:center;white-space:nowrap;vertical-align:middle;-ms-touch-action:manipulation;touch-action:manipulation;cursor:pointer;-webkit-user-select:none;-moz-user-select:none;-ms-user-select:none;user-select:none;background-image:none;border:1px solid transparent;border-radius:4px}.btn.active.focus,.btn.active:focus,.btn.focus,.btn:active.focus,.btn:active:focus,.btn:focus{outline:thin dotted;outline:5px auto -webkit-focus-ring-color;outline-offset:-2px}.btn.focus,.btn:focus,.btn:hover{color:#333;text-decoration:none}.btn.active,.btn:active{background-image:none;outline:0;-webkit-box-shadow:inset 0 3px 5px rgba(0,0,0,.125);box-shadow:inset 0 3px 5px rgba(0,0,0,.125)}.btn.disabled,.btn[disabled],fieldset[disabled] .btn{cursor:not-allowed;filter:alpha(opacity=65);-webkit-box-shadow:none;box-shadow:none;opacity:.65}a.btn.disabled,fieldset[disabled] a.btn{pointer-events:none}.btn-default{color:#333;background-color:#fff;border-color:#ccc}.btn-default.focus,.btn-default:focus{color:#333;background-color:#e6e6e6;border-color:#8c8c8c}.btn-default:hover{color:#333;background-color:#e6e6e6;border-color:#adadad}.btn-default.active,.btn-default:active,.open>.dropdown-toggle.btn-default{color:#333;background-color:#e6e6e6;border-color:#adadad}.btn-default.active.focus,.btn-default.active:focus,.btn-default.active:hover,.btn-default:active.focus,.btn-default:active:focus,.btn-default:active:hover,.open>.dropdown-toggle.btn-default.focus,.open>.dropdown-toggle.btn-default:focus,.open>.dropdown-toggle.btn-default:hover{color:#333;background-color:#d4d4d4;border-color:#8c8c8c}.btn-default.active,.btn-default:active,.open>.dropdown-toggle.btn-default{background-image:none}.btn-default.disabled.focus,.btn-default.disabled:focus,.btn-default.disabled:hover,.btn-default[disabled].focus,.btn-default[disabled]:focus,.btn-default[disabled]:hover,fieldset[disabled] .btn-default.focus,fieldset[disabled] .btn-default:focus,fieldset[disabled] .btn-default:hover{background-color:#fff;border-color:#ccc}.btn-default .badge{color:#fff;background-color:#333}.btn-primary{color:#fff;background-color:#337ab7;border-color:#2e6da4}.btn-primary.focus,.btn-primary:focus{color:#fff;background-color:#286090;border-color:#122b40}.btn-primary:hover{color:#fff;background-color:#286090;border-color:#204d74}.btn-primary.active,.btn-primary:active,.open>.dropdown-toggle.btn-primary{color:#fff;background-color:#286090;border-color:#204d74}.btn-primary.active.focus,.btn-primary.active:focus,.btn-primary.active:hover,.btn-primary:active.focus,.btn-primary:active:focus,.btn-primary:active:hover,.open>.dropdown-toggle.btn-primary.focus,.open>.dropdown-toggle.btn-primary:focus,.open>.dropdown-toggle.btn-primary:hover{color:#fff;background-color:#204d74;border-color:#122b40}.btn-primary.active,.btn-primary:active,.open>.dropdown-toggle.btn-primary{background-image:none}.btn-primary.disabled.focus,.btn-primary.disabled:focus,.btn-primary.disabled:hover,.btn-primary[disabled].focus,.btn-primary[disabled]:focus,.btn-primary[disabled]:hover,fieldset[disabled] .btn-primary.focus,fieldset[disabled] .btn-primary:focus,fieldset[disabled] .btn-primary:hover{background-color:#337ab7;border-color:#2e6da4}.btn-primary .badge{color:#337ab7;background-color:#fff}.btn-success{color:#fff;background-color:#5cb85c;border-color:#4cae4c}.btn-success.focus,.btn-success:focus{color:#fff;background-color:#449d44;border-color:#255625}.btn-success:hover{color:#fff;background-color:#449d44;border-color:#398439}.btn-success.active,.btn-success:active,.open>.dropdown-toggle.btn-success{color:#fff;background-color:#449d44;border-color:#398439}.btn-success.active.focus,.btn-success.active:focus,.btn-success.active:hover,.btn-success:active.focus,.btn-success:active:focus,.btn-success:active:hover,.open>.dropdown-toggle.btn-success.focus,.open>.dropdown-toggle.btn-success:focus,.open>.dropdown-toggle.btn-success:hover{color:#fff;background-color:#398439;border-color:#255625}.btn-success.active,.btn-success:active,.open>.dropdown-toggle.btn-success{background-image:none}.btn-success.disabled.focus,.btn-success.disabled:focus,.btn-success.disabled:hover,.btn-success[disabled].focus,.btn-success[disabled]:focus,.btn-success[disabled]:hover,fieldset[disabled] .btn-success.focus,fieldset[disabled] .btn-success:focus,fieldset[disabled] .btn-success:hover{background-color:#5cb85c;border-color:#4cae4c}.btn-success .badge{color:#5cb85c;background-color:#fff}.btn-info{color:#fff;background-color:#5bc0de;border-color:#46b8da}.btn-info.focus,.btn-info:focus{color:#fff;background-color:#31b0d5;border-color:#1b6d85}.btn-info:hover{color:#fff;background-color:#31b0d5;border-color:#269abc}.btn-info.active,.btn-info:active,.open>.dropdown-toggle.btn-info{color:#fff;background-color:#31b0d5;border-color:#269abc}.btn-info.active.focus,.btn-info.active:focus,.btn-info.active:hover,.btn-info:active.focus,.btn-info:active:focus,.btn-info:active:hover,.open>.dropdown-toggle.btn-info.focus,.open>.dropdown-toggle.btn-info:focus,.open>.dropdown-toggle.btn-info:hover{color:#fff;background-color:#269abc;border-color:#1b6d85}.btn-info.active,.btn-info:active,.open>.dropdown-toggle.btn-info{background-image:none}.btn-info.disabled.focus,.btn-info.disabled:focus,.btn-info.disabled:hover,.btn-info[disabled].focus,.btn-info[disabled]:focus,.btn-info[disabled]:hover,fieldset[disabled] .btn-info.focus,fieldset[disabled] .btn-info:focus,fieldset[disabled] .btn-info:hover{background-color:#5bc0de;border-color:#46b8da}.btn-info .badge{color:#5bc0de;background-color:#fff}.btn-warning{color:#fff;background-color:#f0ad4e;border-color:#eea236}.btn-warning.focus,.btn-warning:focus{color:#fff;background-color:#ec971f;border-color:#985f0d}.btn-warning:hover{color:#fff;background-color:#ec971f;border-color:#d58512}.btn-warning.active,.btn-warning:active,.open>.dropdown-toggle.btn-warning{color:#fff;background-color:#ec971f;border-color:#d58512}.btn-warning.active.focus,.btn-warning.active:focus,.btn-warning.active:hover,.btn-warning:active.focus,.btn-warning:active:focus,.btn-warning:active:hover,.open>.dropdown-toggle.btn-warning.focus,.open>.dropdown-toggle.btn-warning:focus,.open>.dropdown-toggle.btn-warning:hover{color:#fff;background-color:#d58512;border-color:#985f0d}.btn-warning.active,.btn-warning:active,.open>.dropdown-toggle.btn-warning{background-image:none}.btn-warning.disabled.focus,.btn-warning.disabled:focus,.btn-warning.disabled:hover,.btn-warning[disabled].focus,.btn-warning[disabled]:focus,.btn-warning[disabled]:hover,fieldset[disabled] .btn-warning.focus,fieldset[disabled] .btn-warning:focus,fieldset[disabled] .btn-warning:hover{background-color:#f0ad4e;border-color:#eea236}.btn-warning .badge{color:#f0ad4e;background-color:#fff}.btn-danger{color:#fff;background-color:#d9534f;border-color:#d43f3a}.btn-danger.focus,.btn-danger:focus{color:#fff;background-color:#c9302c;border-color:#761c19}.btn-danger:hover{color:#fff;background-color:#c9302c;border-color:#ac2925}.btn-danger.active,.btn-danger:active,.open>.dropdown-toggle.btn-danger{color:#fff;background-color:#c9302c;border-color:#ac2925}.btn-danger.active.focus,.btn-danger.active:focus,.btn-danger.active:hover,.btn-danger:active.focus,.btn-danger:active:focus,.btn-danger:active:hover,.open>.dropdown-toggle.btn-danger.focus,.open>.dropdown-toggle.btn-danger:focus,.open>.dropdown-toggle.btn-danger:hover{color:#fff;background-color:#ac2925;border-color:#761c19}.btn-danger.active,.btn-danger:active,.open>.dropdown-toggle.btn-danger{background-image:none}.btn-danger.disabled.focus,.btn-danger.disabled:focus,.btn-danger.disabled:hover,.btn-danger[disabled].focus,.btn-danger[disabled]:focus,.btn-danger[disabled]:hover,fieldset[disabled] .btn-danger.focus,fieldset[disabled] .btn-danger:focus,fieldset[disabled] .btn-danger:hover{background-color:#d9534f;border-color:#d43f3a}.btn-danger .badge{color:#d9534f;background-color:#fff}.btn-link{font-weight:400;color:#337ab7;border-radius:0}.btn-link,.btn-link.active,.btn-link:active,.btn-link[disabled],fieldset[disabled] .btn-link{background-color:transparent;-webkit-box-shadow:none;box-shadow:none}.btn-link,.btn-link:active,.btn-link:focus,.btn-link:hover{border-color:transparent}.btn-link:focus,.btn-link:hover{color:#23527c;text-decoration:underline;background-color:transparent}.btn-link[disabled]:focus,.btn-link[disabled]:hover,fieldset[disabled] .btn-link:focus,fieldset[disabled] .btn-link:hover{color:#777;text-decoration:none}.btn-group-lg>.btn,.btn-lg{padding:10px 16px;font-size:18px;line-height:1.3333333;border-radius:6px}.btn-group-sm>.btn,.btn-sm{padding:5px 10px;font-size:12px;line-height:1.5;border-radius:3px}.btn-group-xs>.btn,.btn-xs{padding:1px 5px;font-size:12px;line-height:1.5;border-radius:3px}.btn-block{display:block;width:100%}.btn-block+.btn-block{margin-top:5px}input[type=button].btn-block,input[type=reset].btn-block,input[type=submit].btn-block{width:100%}.fade{opacity:0;-webkit-transition:opacity .15s linear;-o-transition:opacity .15s linear;transition:opacity .15s linear}.fade.in{opacity:1}.collapse{display:none}.collapse.in{display:block}tr.collapse.in{display:table-row}tbody.collapse.in{display:table-row-group}.collapsing{position:relative;height:0;overflow:hidden;-webkit-transition-timing-function:ease;-o-transition-timing-function:ease;transition-timing-function:ease;-webkit-transition-duration:.35s;-o-transition-duration:.35s;transition-duration:.35s;-webkit-transition-property:height,visibility;-o-transition-property:height,visibility;transition-property:height,visibility}.caret{display:inline-block;width:0;height:0;margin-left:2px;vertical-align:middle;border-top:4px dashed;border-top:4px solid\\9;border-right:4px solid transparent;border-left:4px solid transparent}.dropdown,.dropup{position:relative}.dropdown-toggle:focus{outline:0}.dropdown-menu{position:absolute;top:100%;left:0;z-index:1000;display:none;float:left;min-width:160px;padding:5px 0;margin:2px 0 0;font-size:14px;text-align:left;list-style:none;background-color:#fff;-webkit-background-clip:padding-box;background-clip:padding-box;border:1px solid #ccc;border:1px solid rgba(0,0,0,.15);border-radius:4px;-webkit-box-shadow:0 6px 12px rgba(0,0,0,.175);box-shadow:0 6px 12px rgba(0,0,0,.175)}.dropdown-menu.pull-right{right:0;left:auto}.dropdown-menu .divider{height:1px;margin:9px 0;overflow:hidden;background-color:#e5e5e5}.dropdown-menu>li>a{display:block;padding:3px 20px;clear:both;font-weight:400;line-height:1.42857143;color:#333;white-space:nowrap}.dropdown-menu>li>a:focus,.dropdown-menu>li>a:hover{color:#262626;text-decoration:none;background-color:#f5f5f5}.dropdown-menu>.active>a,.dropdown-menu>.active>a:focus,.dropdown-menu>.active>a:hover{color:#fff;text-decoration:none;background-color:#337ab7;outline:0}.dropdown-menu>.disabled>a,.dropdown-menu>.disabled>a:focus,.dropdown-menu>.disabled>a:hover{color:#777}.dropdown-menu>.disabled>a:focus,.dropdown-menu>.disabled>a:hover{text-decoration:none;cursor:not-allowed;background-color:transparent;background-image:none;filter:progid:DXImageTransform.Microsoft.gradient(enabled=false)}.open>.dropdown-menu{display:block}.open>a{outline:0}.dropdown-menu-right{right:0;left:auto}.dropdown-menu-left{right:auto;left:0}.dropdown-header{display:block;padding:3px 20px;font-size:12px;line-height:1.42857143;color:#777;white-space:nowrap}.dropdown-backdrop{position:fixed;top:0;right:0;bottom:0;left:0;z-index:990}.pull-right>.dropdown-menu{right:0;left:auto}.dropup .caret,.navbar-fixed-bottom .dropdown .caret{content:\"\";border-top:0;border-bottom:4px dashed;border-bottom:4px solid\\9}.dropup .dropdown-menu,.navbar-fixed-bottom .dropdown .dropdown-menu{top:auto;bottom:100%;margin-bottom:2px}@media (min-width:768px){.navbar-right .dropdown-menu{right:0;left:auto}.navbar-right .dropdown-menu-left{right:auto;left:0}}.btn-group,.btn-group-vertical{position:relative;display:inline-block;vertical-align:middle}.btn-group-vertical>.btn,.btn-group>.btn{position:relative;float:left}.btn-group-vertical>.btn.active,.btn-group-vertical>.btn:active,.btn-group-vertical>.btn:focus,.btn-group-vertical>.btn:hover,.btn-group>.btn.active,.btn-group>.btn:active,.btn-group>.btn:focus,.btn-group>.btn:hover{z-index:2}.btn-group .btn+.btn,.btn-group .btn+.btn-group,.btn-group .btn-group+.btn,.btn-group .btn-group+.btn-group{margin-left:-1px}.btn-toolbar{margin-left:-5px}.btn-toolbar .btn,.btn-toolbar .btn-group,.btn-toolbar .input-group{float:left}.btn-toolbar>.btn,.btn-toolbar>.btn-group,.btn-toolbar>.input-group{margin-left:5px}.btn-group>.btn:not(:first-child):not(:last-child):not(.dropdown-toggle){border-radius:0}.btn-group>.btn:first-child{margin-left:0}.btn-group>.btn:first-child:not(:last-child):not(.dropdown-toggle){border-top-right-radius:0;border-bottom-right-radius:0}.btn-group>.btn:last-child:not(:first-child),.btn-group>.dropdown-toggle:not(:first-child){border-top-left-radius:0;border-bottom-left-radius:0}.btn-group>.btn-group{float:left}.btn-group>.btn-group:not(:first-child):not(:last-child)>.btn{border-radius:0}.btn-group>.btn-group:first-child:not(:last-child)>.btn:last-child,.btn-group>.btn-group:first-child:not(:last-child)>.dropdown-toggle{border-top-right-radius:0;border-bottom-right-radius:0}.btn-group>.btn-group:last-child:not(:first-child)>.btn:first-child{border-top-left-radius:0;border-bottom-left-radius:0}.btn-group .dropdown-toggle:active,.btn-group.open .dropdown-toggle{outline:0}.btn-group>.btn+.dropdown-toggle{padding-right:8px;padding-left:8px}.btn-group>.btn-lg+.dropdown-toggle{padding-right:12px;padding-left:12px}.btn-group.open .dropdown-toggle{-webkit-box-shadow:inset 0 3px 5px rgba(0,0,0,.125);box-shadow:inset 0 3px 5px rgba(0,0,0,.125)}.btn-group.open .dropdown-toggle.btn-link{-webkit-box-shadow:none;box-shadow:none}.btn .caret{margin-left:0}.btn-lg .caret{border-width:5px 5px 0;border-bottom-width:0}.dropup .btn-lg .caret{border-width:0 5px 5px}.btn-group-vertical>.btn,.btn-group-vertical>.btn-group,.btn-group-vertical>.btn-group>.btn{display:block;float:none;width:100%;max-width:100%}.btn-group-vertical>.btn-group>.btn{float:none}.btn-group-vertical>.btn+.btn,.btn-group-vertical>.btn+.btn-group,.btn-group-vertical>.btn-group+.btn,.btn-group-vertical>.btn-group+.btn-group{margin-top:-1px;margin-left:0}.btn-group-vertical>.btn:not(:first-child):not(:last-child){border-radius:0}.btn-group-vertical>.btn:first-child:not(:last-child){border-top-left-radius:4px;border-top-right-radius:4px;border-bottom-right-radius:0;border-bottom-left-radius:0}.btn-group-vertical>.btn:last-child:not(:first-child){border-top-left-radius:0;border-top-right-radius:0;border-bottom-right-radius:4px;border-bottom-left-radius:4px}.btn-group-vertical>.btn-group:not(:first-child):not(:last-child)>.btn{border-radius:0}.btn-group-vertical>.btn-group:first-child:not(:last-child)>.btn:last-child,.btn-group-vertical>.btn-group:first-child:not(:last-child)>.dropdown-toggle{border-bottom-right-radius:0;border-bottom-left-radius:0}.btn-group-vertical>.btn-group:last-child:not(:first-child)>.btn:first-child{border-top-left-radius:0;border-top-right-radius:0}.btn-group-justified{display:table;width:100%;table-layout:fixed;border-collapse:separate}.btn-group-justified>.btn,.btn-group-justified>.btn-group{display:table-cell;float:none;width:1%}.btn-group-justified>.btn-group .btn{width:100%}.btn-group-justified>.btn-group .dropdown-menu{left:auto}[data-toggle=buttons]>.btn input[type=checkbox],[data-toggle=buttons]>.btn input[type=radio],[data-toggle=buttons]>.btn-group>.btn input[type=checkbox],[data-toggle=buttons]>.btn-group>.btn input[type=radio]{position:absolute;clip:rect(0,0,0,0);pointer-events:none}.input-group{position:relative;display:table;border-collapse:separate}.input-group[class*=col-]{float:none;padding-right:0;padding-left:0}.input-group .form-control{position:relative;z-index:2;float:left;width:100%;margin-bottom:0}.input-group .form-control:focus{z-index:3}.input-group-lg>.form-control,.input-group-lg>.input-group-addon,.input-group-lg>.input-group-btn>.btn{height:46px;padding:10px 16px;font-size:18px;line-height:1.3333333;border-radius:6px}select.input-group-lg>.form-control,select.input-group-lg>.input-group-addon,select.input-group-lg>.input-group-btn>.btn{height:46px;line-height:46px}select[multiple].input-group-lg>.form-control,select[multiple].input-group-lg>.input-group-addon,select[multiple].input-group-lg>.input-group-btn>.btn,textarea.input-group-lg>.form-control,textarea.input-group-lg>.input-group-addon,textarea.input-group-lg>.input-group-btn>.btn{height:auto}.input-group-sm>.form-control,.input-group-sm>.input-group-addon,.input-group-sm>.input-group-btn>.btn{height:30px;padding:5px 10px;font-size:12px;line-height:1.5;border-radius:3px}select.input-group-sm>.form-control,select.input-group-sm>.input-group-addon,select.input-group-sm>.input-group-btn>.btn{height:30px;line-height:30px}select[multiple].input-group-sm>.form-control,select[multiple].input-group-sm>.input-group-addon,select[multiple].input-group-sm>.input-group-btn>.btn,textarea.input-group-sm>.form-control,textarea.input-group-sm>.input-group-addon,textarea.input-group-sm>.input-group-btn>.btn{height:auto}.input-group .form-control,.input-group-addon,.input-group-btn{display:table-cell}.input-group .form-control:not(:first-child):not(:last-child),.input-group-addon:not(:first-child):not(:last-child),.input-group-btn:not(:first-child):not(:last-child){border-radius:0}.input-group-addon,.input-group-btn{width:1%;white-space:nowrap;vertical-align:middle}.input-group-addon{padding:6px 12px;font-size:14px;font-weight:400;line-height:1;color:#555;text-align:center;background-color:#eee;border:1px solid #ccc;border-radius:4px}.input-group-addon.input-sm{padding:5px 10px;font-size:12px;border-radius:3px}.input-group-addon.input-lg{padding:10px 16px;font-size:18px;border-radius:6px}.input-group-addon input[type=checkbox],.input-group-addon input[type=radio]{margin-top:0}.input-group .form-control:first-child,.input-group-addon:first-child,.input-group-btn:first-child>.btn,.input-group-btn:first-child>.btn-group>.btn,.input-group-btn:first-child>.dropdown-toggle,.input-group-btn:last-child>.btn-group:not(:last-child)>.btn,.input-group-btn:last-child>.btn:not(:last-child):not(.dropdown-toggle){border-top-right-radius:0;border-bottom-right-radius:0}.input-group-addon:first-child{border-right:0}.input-group .form-control:last-child,.input-group-addon:last-child,.input-group-btn:first-child>.btn-group:not(:first-child)>.btn,.input-group-btn:first-child>.btn:not(:first-child),.input-group-btn:last-child>.btn,.input-group-btn:last-child>.btn-group>.btn,.input-group-btn:last-child>.dropdown-toggle{border-top-left-radius:0;border-bottom-left-radius:0}.input-group-addon:last-child{border-left:0}.input-group-btn{position:relative;font-size:0;white-space:nowrap}.input-group-btn>.btn{position:relative}.input-group-btn>.btn+.btn{margin-left:-1px}.input-group-btn>.btn:active,.input-group-btn>.btn:focus,.input-group-btn>.btn:hover{z-index:2}.input-group-btn:first-child>.btn,.input-group-btn:first-child>.btn-group{margin-right:-1px}.input-group-btn:last-child>.btn,.input-group-btn:last-child>.btn-group{z-index:2;margin-left:-1px}.nav{padding-left:0;margin-bottom:0;list-style:none}.nav>li{position:relative;display:block}.nav>li>a{position:relative;display:block;padding:10px 15px}.nav>li>a:focus,.nav>li>a:hover{text-decoration:none;background-color:#eee}.nav>li.disabled>a{color:#777}.nav>li.disabled>a:focus,.nav>li.disabled>a:hover{color:#777;text-decoration:none;cursor:not-allowed;background-color:transparent}.nav .open>a,.nav .open>a:focus,.nav .open>a:hover{background-color:#eee;border-color:#337ab7}.nav .nav-divider{height:1px;margin:9px 0;overflow:hidden;background-color:#e5e5e5}.nav>li>a>img{max-width:none}.nav-tabs{border-bottom:1px solid #ddd}.nav-tabs>li{float:left;margin-bottom:-1px}.nav-tabs>li>a{margin-right:2px;line-height:1.42857143;border:1px solid transparent;border-radius:4px 4px 0 0}.nav-tabs>li>a:hover{border-color:#eee #eee #ddd}.nav-tabs>li.active>a,.nav-tabs>li.active>a:focus,.nav-tabs>li.active>a:hover{color:#555;cursor:default;background-color:#fff;border:1px solid #ddd;border-bottom-color:transparent}.nav-tabs.nav-justified{width:100%;border-bottom:0}.nav-tabs.nav-justified>li{float:none}.nav-tabs.nav-justified>li>a{margin-bottom:5px;text-align:center}.nav-tabs.nav-justified>.dropdown .dropdown-menu{top:auto;left:auto}@media (min-width:768px){.nav-tabs.nav-justified>li{display:table-cell;width:1%}.nav-tabs.nav-justified>li>a{margin-bottom:0}}.nav-tabs.nav-justified>li>a{margin-right:0;border-radius:4px}.nav-tabs.nav-justified>.active>a,.nav-tabs.nav-justified>.active>a:focus,.nav-tabs.nav-justified>.active>a:hover{border:1px solid #ddd}@media (min-width:768px){.nav-tabs.nav-justified>li>a{border-bottom:1px solid #ddd;border-radius:4px 4px 0 0}.nav-tabs.nav-justified>.active>a,.nav-tabs.nav-justified>.active>a:focus,.nav-tabs.nav-justified>.active>a:hover{border-bottom-color:#fff}}.nav-pills>li{float:left}.nav-pills>li>a{border-radius:4px}.nav-pills>li+li{margin-left:2px}.nav-pills>li.active>a,.nav-pills>li.active>a:focus,.nav-pills>li.active>a:hover{color:#fff;background-color:#337ab7}.nav-stacked>li{float:none}.nav-stacked>li+li{margin-top:2px;margin-left:0}.nav-justified{width:100%}.nav-justified>li{float:none}.nav-justified>li>a{margin-bottom:5px;text-align:center}.nav-justified>.dropdown .dropdown-menu{top:auto;left:auto}@media (min-width:768px){.nav-justified>li{display:table-cell;width:1%}.nav-justified>li>a{margin-bottom:0}}.nav-tabs-justified{border-bottom:0}.nav-tabs-justified>li>a{margin-right:0;border-radius:4px}.nav-tabs-justified>.active>a,.nav-tabs-justified>.active>a:focus,.nav-tabs-justified>.active>a:hover{border:1px solid #ddd}@media (min-width:768px){.nav-tabs-justified>li>a{border-bottom:1px solid #ddd;border-radius:4px 4px 0 0}.nav-tabs-justified>.active>a,.nav-tabs-justified>.active>a:focus,.nav-tabs-justified>.active>a:hover{border-bottom-color:#fff}}.tab-content>.tab-pane{display:none}.tab-content>.active{display:block}.nav-tabs .dropdown-menu{margin-top:-1px;border-top-left-radius:0;border-top-right-radius:0}.navbar{position:relative;min-height:50px;margin-bottom:20px;border:1px solid transparent}@media (min-width:768px){.navbar{border-radius:4px}}@media (min-width:768px){.navbar-header{float:left}}.navbar-collapse{padding-right:15px;padding-left:15px;overflow-x:visible;-webkit-overflow-scrolling:touch;border-top:1px solid transparent;-webkit-box-shadow:inset 0 1px 0 rgba(255,255,255,.1);box-shadow:inset 0 1px 0 rgba(255,255,255,.1)}.navbar-collapse.in{overflow-y:auto}@media (min-width:768px){.navbar-collapse{width:auto;border-top:0;-webkit-box-shadow:none;box-shadow:none}.navbar-collapse.collapse{display:block!important;height:auto!important;padding-bottom:0;overflow:visible!important}.navbar-collapse.in{overflow-y:visible}.navbar-fixed-bottom .navbar-collapse,.navbar-fixed-top .navbar-collapse,.navbar-static-top .navbar-collapse{padding-right:0;padding-left:0}}.navbar-fixed-bottom .navbar-collapse,.navbar-fixed-top .navbar-collapse{max-height:340px}@media (max-device-width:480px) and (orientation:landscape){.navbar-fixed-bottom .navbar-collapse,.navbar-fixed-top .navbar-collapse{max-height:200px}}.container-fluid>.navbar-collapse,.container-fluid>.navbar-header,.container>.navbar-collapse,.container>.navbar-header{margin-right:-15px;margin-left:-15px}@media (min-width:768px){.container-fluid>.navbar-collapse,.container-fluid>.navbar-header,.container>.navbar-collapse,.container>.navbar-header{margin-right:0;margin-left:0}}.navbar-static-top{z-index:1000;border-width:0 0 1px}@media (min-width:768px){.navbar-static-top{border-radius:0}}.navbar-fixed-bottom,.navbar-fixed-top{position:fixed;right:0;left:0;z-index:1030}@media (min-width:768px){.navbar-fixed-bottom,.navbar-fixed-top{border-radius:0}}.navbar-fixed-top{top:0;border-width:0 0 1px}.navbar-fixed-bottom{bottom:0;margin-bottom:0;border-width:1px 0 0}.navbar-brand{float:left;height:50px;padding:15px 15px;font-size:18px;line-height:20px}.navbar-brand:focus,.navbar-brand:hover{text-decoration:none}.navbar-brand>img{display:block}@media (min-width:768px){.navbar>.container .navbar-brand,.navbar>.container-fluid .navbar-brand{margin-left:-15px}}.navbar-toggle{position:relative;float:right;padding:9px 10px;margin-top:8px;margin-right:15px;margin-bottom:8px;background-color:transparent;background-image:none;border:1px solid transparent;border-radius:4px}.navbar-toggle:focus{outline:0}.navbar-toggle .icon-bar{display:block;width:22px;height:2px;border-radius:1px}.navbar-toggle .icon-bar+.icon-bar{margin-top:4px}@media (min-width:768px){.navbar-toggle{display:none}}.navbar-nav{margin:7.5px -15px}.navbar-nav>li>a{padding-top:10px;padding-bottom:10px;line-height:20px}@media (max-width:767px){.navbar-nav .open .dropdown-menu{position:static;float:none;width:auto;margin-top:0;background-color:transparent;border:0;-webkit-box-shadow:none;box-shadow:none}.navbar-nav .open .dropdown-menu .dropdown-header,.navbar-nav .open .dropdown-menu>li>a{padding:5px 15px 5px 25px}.navbar-nav .open .dropdown-menu>li>a{line-height:20px}.navbar-nav .open .dropdown-menu>li>a:focus,.navbar-nav .open .dropdown-menu>li>a:hover{background-image:none}}@media (min-width:768px){.navbar-nav{float:left;margin:0}.navbar-nav>li{float:left}.navbar-nav>li>a{padding-top:15px;padding-bottom:15px}}.navbar-form{padding:10px 15px;margin-top:8px;margin-right:-15px;margin-bottom:8px;margin-left:-15px;border-top:1px solid transparent;border-bottom:1px solid transparent;-webkit-box-shadow:inset 0 1px 0 rgba(255,255,255,.1),0 1px 0 rgba(255,255,255,.1);box-shadow:inset 0 1px 0 rgba(255,255,255,.1),0 1px 0 rgba(255,255,255,.1)}@media (min-width:768px){.navbar-form .form-group{display:inline-block;margin-bottom:0;vertical-align:middle}.navbar-form .form-control{display:inline-block;width:auto;vertical-align:middle}.navbar-form .form-control-static{display:inline-block}.navbar-form .input-group{display:inline-table;vertical-align:middle}.navbar-form .input-group .form-control,.navbar-form .input-group .input-group-addon,.navbar-form .input-group .input-group-btn{width:auto}.navbar-form .input-group>.form-control{width:100%}.navbar-form .control-label{margin-bottom:0;vertical-align:middle}.navbar-form .checkbox,.navbar-form .radio{display:inline-block;margin-top:0;margin-bottom:0;vertical-align:middle}.navbar-form .checkbox label,.navbar-form .radio label{padding-left:0}.navbar-form .checkbox input[type=checkbox],.navbar-form .radio input[type=radio]{position:relative;margin-left:0}.navbar-form .has-feedback .form-control-feedback{top:0}}@media (max-width:767px){.navbar-form .form-group{margin-bottom:5px}.navbar-form .form-group:last-child{margin-bottom:0}}@media (min-width:768px){.navbar-form{width:auto;padding-top:0;padding-bottom:0;margin-right:0;margin-left:0;border:0;-webkit-box-shadow:none;box-shadow:none}}.navbar-nav>li>.dropdown-menu{margin-top:0;border-top-left-radius:0;border-top-right-radius:0}.navbar-fixed-bottom .navbar-nav>li>.dropdown-menu{margin-bottom:0;border-top-left-radius:4px;border-top-right-radius:4px;border-bottom-right-radius:0;border-bottom-left-radius:0}.navbar-btn{margin-top:8px;margin-bottom:8px}.navbar-btn.btn-sm{margin-top:10px;margin-bottom:10px}.navbar-btn.btn-xs{margin-top:14px;margin-bottom:14px}.navbar-text{margin-top:15px;margin-bottom:15px}@media (min-width:768px){.navbar-text{float:left;margin-right:15px;margin-left:15px}}@media (min-width:768px){.navbar-left{float:left!important}.navbar-right{float:right!important;margin-right:-15px}.navbar-right~.navbar-right{margin-right:0}}.navbar-default{background-color:#f8f8f8;border-color:#e7e7e7}.navbar-default .navbar-brand{color:#777}.navbar-default .navbar-brand:focus,.navbar-default .navbar-brand:hover{color:#5e5e5e;background-color:transparent}.navbar-default .navbar-text{color:#777}.navbar-default .navbar-nav>li>a{color:#777}.navbar-default .navbar-nav>li>a:focus,.navbar-default .navbar-nav>li>a:hover{color:#333;background-color:transparent}.navbar-default .navbar-nav>.active>a,.navbar-default .navbar-nav>.active>a:focus,.navbar-default .navbar-nav>.active>a:hover{color:#555;background-color:#e7e7e7}.navbar-default .navbar-nav>.disabled>a,.navbar-default .navbar-nav>.disabled>a:focus,.navbar-default .navbar-nav>.disabled>a:hover{color:#ccc;background-color:transparent}.navbar-default .navbar-toggle{border-color:#ddd}.navbar-default .navbar-toggle:focus,.navbar-default .navbar-toggle:hover{background-color:#ddd}.navbar-default .navbar-toggle .icon-bar{background-color:#888}.navbar-default .navbar-collapse,.navbar-default .navbar-form{border-color:#e7e7e7}.navbar-default .navbar-nav>.open>a,.navbar-default .navbar-nav>.open>a:focus,.navbar-default .navbar-nav>.open>a:hover{color:#555;background-color:#e7e7e7}@media (max-width:767px){.navbar-default .navbar-nav .open .dropdown-menu>li>a{color:#777}.navbar-default .navbar-nav .open .dropdown-menu>li>a:focus,.navbar-default .navbar-nav .open .dropdown-menu>li>a:hover{color:#333;background-color:transparent}.navbar-default .navbar-nav .open .dropdown-menu>.active>a,.navbar-default .navbar-nav .open .dropdown-menu>.active>a:focus,.navbar-default .navbar-nav .open .dropdown-menu>.active>a:hover{color:#555;background-color:#e7e7e7}.navbar-default .navbar-nav .open .dropdown-menu>.disabled>a,.navbar-default .navbar-nav .open .dropdown-menu>.disabled>a:focus,.navbar-default .navbar-nav .open .dropdown-menu>.disabled>a:hover{color:#ccc;background-color:transparent}}.navbar-default .navbar-link{color:#777}.navbar-default .navbar-link:hover{color:#333}.navbar-default .btn-link{color:#777}.navbar-default .btn-link:focus,.navbar-default .btn-link:hover{color:#333}.navbar-default .btn-link[disabled]:focus,.navbar-default .btn-link[disabled]:hover,fieldset[disabled] .navbar-default .btn-link:focus,fieldset[disabled] .navbar-default .btn-link:hover{color:#ccc}.navbar-inverse{background-color:#222;border-color:#080808}.navbar-inverse .navbar-brand{color:#9d9d9d}.navbar-inverse .navbar-brand:focus,.navbar-inverse .navbar-brand:hover{color:#fff;background-color:transparent}.navbar-inverse .navbar-text{color:#9d9d9d}.navbar-inverse .navbar-nav>li>a{color:#9d9d9d}.navbar-inverse .navbar-nav>li>a:focus,.navbar-inverse .navbar-nav>li>a:hover{color:#fff;background-color:transparent}.navbar-inverse .navbar-nav>.active>a,.navbar-inverse .navbar-nav>.active>a:focus,.navbar-inverse .navbar-nav>.active>a:hover{color:#fff;background-color:#080808}.navbar-inverse .navbar-nav>.disabled>a,.navbar-inverse .navbar-nav>.disabled>a:focus,.navbar-inverse .navbar-nav>.disabled>a:hover{color:#444;background-color:transparent}.navbar-inverse .navbar-toggle{border-color:#333}.navbar-inverse .navbar-toggle:focus,.navbar-inverse .navbar-toggle:hover{background-color:#333}.navbar-inverse .navbar-toggle .icon-bar{background-color:#fff}.navbar-inverse .navbar-collapse,.navbar-inverse .navbar-form{border-color:#101010}.navbar-inverse .navbar-nav>.open>a,.navbar-inverse .navbar-nav>.open>a:focus,.navbar-inverse .navbar-nav>.open>a:hover{color:#fff;background-color:#080808}@media (max-width:767px){.navbar-inverse .navbar-nav .open .dropdown-menu>.dropdown-header{border-color:#080808}.navbar-inverse .navbar-nav .open .dropdown-menu .divider{background-color:#080808}.navbar-inverse .navbar-nav .open .dropdown-menu>li>a{color:#9d9d9d}.navbar-inverse .navbar-nav .open .dropdown-menu>li>a:focus,.navbar-inverse .navbar-nav .open .dropdown-menu>li>a:hover{color:#fff;background-color:transparent}.navbar-inverse .navbar-nav .open .dropdown-menu>.active>a,.navbar-inverse .navbar-nav .open .dropdown-menu>.active>a:focus,.navbar-inverse .navbar-nav .open .dropdown-menu>.active>a:hover{color:#fff;background-color:#080808}.navbar-inverse .navbar-nav .open .dropdown-menu>.disabled>a,.navbar-inverse .navbar-nav .open .dropdown-menu>.disabled>a:focus,.navbar-inverse .navbar-nav .open .dropdown-menu>.disabled>a:hover{color:#444;background-color:transparent}}.navbar-inverse .navbar-link{color:#9d9d9d}.navbar-inverse .navbar-link:hover{color:#fff}.navbar-inverse .btn-link{color:#9d9d9d}.navbar-inverse .btn-link:focus,.navbar-inverse .btn-link:hover{color:#fff}.navbar-inverse .btn-link[disabled]:focus,.navbar-inverse .btn-link[disabled]:hover,fieldset[disabled] .navbar-inverse .btn-link:focus,fieldset[disabled] .navbar-inverse .btn-link:hover{color:#444}.breadcrumb{padding:8px 15px;margin-bottom:20px;list-style:none;background-color:#f5f5f5;border-radius:4px}.breadcrumb>li{display:inline-block}.breadcrumb>li+li:before{padding:0 5px;color:#ccc;content:\"/\\A0\"}.breadcrumb>.active{color:#777}.pagination{display:inline-block;padding-left:0;margin:20px 0;border-radius:4px}.pagination>li{display:inline}.pagination>li>a,.pagination>li>span{position:relative;float:left;padding:6px 12px;margin-left:-1px;line-height:1.42857143;color:#337ab7;text-decoration:none;background-color:#fff;border:1px solid #ddd}.pagination>li:first-child>a,.pagination>li:first-child>span{margin-left:0;border-top-left-radius:4px;border-bottom-left-radius:4px}.pagination>li:last-child>a,.pagination>li:last-child>span{border-top-right-radius:4px;border-bottom-right-radius:4px}.pagination>li>a:focus,.pagination>li>a:hover,.pagination>li>span:focus,.pagination>li>span:hover{z-index:2;color:#23527c;background-color:#eee;border-color:#ddd}.pagination>.active>a,.pagination>.active>a:focus,.pagination>.active>a:hover,.pagination>.active>span,.pagination>.active>span:focus,.pagination>.active>span:hover{z-index:3;color:#fff;cursor:default;background-color:#337ab7;border-color:#337ab7}.pagination>.disabled>a,.pagination>.disabled>a:focus,.pagination>.disabled>a:hover,.pagination>.disabled>span,.pagination>.disabled>span:focus,.pagination>.disabled>span:hover{color:#777;cursor:not-allowed;background-color:#fff;border-color:#ddd}.pagination-lg>li>a,.pagination-lg>li>span{padding:10px 16px;font-size:18px;line-height:1.3333333}.pagination-lg>li:first-child>a,.pagination-lg>li:first-child>span{border-top-left-radius:6px;border-bottom-left-radius:6px}.pagination-lg>li:last-child>a,.pagination-lg>li:last-child>span{border-top-right-radius:6px;border-bottom-right-radius:6px}.pagination-sm>li>a,.pagination-sm>li>span{padding:5px 10px;font-size:12px;line-height:1.5}.pagination-sm>li:first-child>a,.pagination-sm>li:first-child>span{border-top-left-radius:3px;border-bottom-left-radius:3px}.pagination-sm>li:last-child>a,.pagination-sm>li:last-child>span{border-top-right-radius:3px;border-bottom-right-radius:3px}.pager{padding-left:0;margin:20px 0;text-align:center;list-style:none}.pager li{display:inline}.pager li>a,.pager li>span{display:inline-block;padding:5px 14px;background-color:#fff;border:1px solid #ddd;border-radius:15px}.pager li>a:focus,.pager li>a:hover{text-decoration:none;background-color:#eee}.pager .next>a,.pager .next>span{float:right}.pager .previous>a,.pager .previous>span{float:left}.pager .disabled>a,.pager .disabled>a:focus,.pager .disabled>a:hover,.pager .disabled>span{color:#777;cursor:not-allowed;background-color:#fff}.label{display:inline;padding:.2em .6em .3em;font-size:75%;font-weight:700;line-height:1;color:#fff;text-align:center;white-space:nowrap;vertical-align:baseline;border-radius:.25em}a.label:focus,a.label:hover{color:#fff;text-decoration:none;cursor:pointer}.label:empty{display:none}.btn .label{position:relative;top:-1px}.label-default{background-color:#777}.label-default[href]:focus,.label-default[href]:hover{background-color:#5e5e5e}.label-primary{background-color:#337ab7}.label-primary[href]:focus,.label-primary[href]:hover{background-color:#286090}.label-success{background-color:#5cb85c}.label-success[href]:focus,.label-success[href]:hover{background-color:#449d44}.label-info{background-color:#5bc0de}.label-info[href]:focus,.label-info[href]:hover{background-color:#31b0d5}.label-warning{background-color:#f0ad4e}.label-warning[href]:focus,.label-warning[href]:hover{background-color:#ec971f}.label-danger{background-color:#d9534f}.label-danger[href]:focus,.label-danger[href]:hover{background-color:#c9302c}.badge{display:inline-block;min-width:10px;padding:3px 7px;font-size:12px;font-weight:700;line-height:1;color:#fff;text-align:center;white-space:nowrap;vertical-align:middle;background-color:#777;border-radius:10px}.badge:empty{display:none}.btn .badge{position:relative;top:-1px}.btn-group-xs>.btn .badge,.btn-xs .badge{top:0;padding:1px 5px}a.badge:focus,a.badge:hover{color:#fff;text-decoration:none;cursor:pointer}.list-group-item.active>.badge,.nav-pills>.active>a>.badge{color:#337ab7;background-color:#fff}.list-group-item>.badge{float:right}.list-group-item>.badge+.badge{margin-right:5px}.nav-pills>li>a>.badge{margin-left:3px}.jumbotron{padding-top:30px;padding-bottom:30px;margin-bottom:30px;color:inherit;background-color:#eee}.jumbotron .h1,.jumbotron h1{color:inherit}.jumbotron p{margin-bottom:15px;font-size:21px;font-weight:200}.jumbotron>hr{border-top-color:#d5d5d5}.container .jumbotron,.container-fluid .jumbotron{padding-right:15px;padding-left:15px;border-radius:6px}.jumbotron .container{max-width:100%}@media screen and (min-width:768px){.jumbotron{padding-top:48px;padding-bottom:48px}.container .jumbotron,.container-fluid .jumbotron{padding-right:60px;padding-left:60px}.jumbotron .h1,.jumbotron h1{font-size:63px}}.thumbnail{display:block;padding:4px;margin-bottom:20px;line-height:1.42857143;background-color:#fff;border:1px solid #ddd;border-radius:4px;-webkit-transition:border .2s ease-in-out;-o-transition:border .2s ease-in-out;transition:border .2s ease-in-out}.thumbnail a>img,.thumbnail>img{margin-right:auto;margin-left:auto}a.thumbnail.active,a.thumbnail:focus,a.thumbnail:hover{border-color:#337ab7}.thumbnail .caption{padding:9px;color:#333}.alert{padding:15px;margin-bottom:20px;border:1px solid transparent;border-radius:4px}.alert h4{margin-top:0;color:inherit}.alert .alert-link{font-weight:700}.alert>p,.alert>ul{margin-bottom:0}.alert>p+p{margin-top:5px}.alert-dismissable,.alert-dismissible{padding-right:35px}.alert-dismissable .close,.alert-dismissible .close{position:relative;top:-2px;right:-21px;color:inherit}.alert-success{color:#3c763d;background-color:#dff0d8;border-color:#d6e9c6}.alert-success hr{border-top-color:#c9e2b3}.alert-success .alert-link{color:#2b542c}.alert-info{color:#31708f;background-color:#d9edf7;border-color:#bce8f1}.alert-info hr{border-top-color:#a6e1ec}.alert-info .alert-link{color:#245269}.alert-warning{color:#8a6d3b;background-color:#fcf8e3;border-color:#faebcc}.alert-warning hr{border-top-color:#f7e1b5}.alert-warning .alert-link{color:#66512c}.alert-danger{color:#a94442;background-color:#f2dede;border-color:#ebccd1}.alert-danger hr{border-top-color:#e4b9c0}.alert-danger .alert-link{color:#843534}@-webkit-keyframes progress-bar-stripes{from{background-position:40px 0}to{background-position:0 0}}@-o-keyframes progress-bar-stripes{from{background-position:40px 0}to{background-position:0 0}}@keyframes progress-bar-stripes{from{background-position:40px 0}to{background-position:0 0}}.progress{height:20px;margin-bottom:20px;overflow:hidden;background-color:#f5f5f5;border-radius:4px;-webkit-box-shadow:inset 0 1px 2px rgba(0,0,0,.1);box-shadow:inset 0 1px 2px rgba(0,0,0,.1)}.progress-bar{float:left;width:0;height:100%;font-size:12px;line-height:20px;color:#fff;text-align:center;background-color:#337ab7;-webkit-box-shadow:inset 0 -1px 0 rgba(0,0,0,.15);box-shadow:inset 0 -1px 0 rgba(0,0,0,.15);-webkit-transition:width .6s ease;-o-transition:width .6s ease;transition:width .6s ease}.progress-bar-striped,.progress-striped .progress-bar{background-image:-webkit-linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent);background-image:-o-linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent);background-image:linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent);-webkit-background-size:40px 40px;background-size:40px 40px}.progress-bar.active,.progress.active .progress-bar{-webkit-animation:progress-bar-stripes 2s linear infinite;-o-animation:progress-bar-stripes 2s linear infinite;animation:progress-bar-stripes 2s linear infinite}.progress-bar-success{background-color:#5cb85c}.progress-striped .progress-bar-success{background-image:-webkit-linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent);background-image:-o-linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent);background-image:linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent)}.progress-bar-info{background-color:#5bc0de}.progress-striped .progress-bar-info{background-image:-webkit-linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent);background-image:-o-linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent);background-image:linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent)}.progress-bar-warning{background-color:#f0ad4e}.progress-striped .progress-bar-warning{background-image:-webkit-linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent);background-image:-o-linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent);background-image:linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent)}.progress-bar-danger{background-color:#d9534f}.progress-striped .progress-bar-danger{background-image:-webkit-linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent);background-image:-o-linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent);background-image:linear-gradient(45deg,rgba(255,255,255,.15) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.15) 50%,rgba(255,255,255,.15) 75%,transparent 75%,transparent)}.media{margin-top:15px}.media:first-child{margin-top:0}.media,.media-body{overflow:hidden;zoom:1}.media-body{width:10000px}.media-object{display:block}.media-object.img-thumbnail{max-width:none}.media-right,.media>.pull-right{padding-left:10px}.media-left,.media>.pull-left{padding-right:10px}.media-body,.media-left,.media-right{display:table-cell;vertical-align:top}.media-middle{vertical-align:middle}.media-bottom{vertical-align:bottom}.media-heading{margin-top:0;margin-bottom:5px}.media-list{padding-left:0;list-style:none}.list-group{padding-left:0;margin-bottom:20px}.list-group-item{position:relative;display:block;padding:10px 15px;margin-bottom:-1px;background-color:#fff;border:1px solid #ddd}.list-group-item:first-child{border-top-left-radius:4px;border-top-right-radius:4px}.list-group-item:last-child{margin-bottom:0;border-bottom-right-radius:4px;border-bottom-left-radius:4px}a.list-group-item,button.list-group-item{color:#555}a.list-group-item .list-group-item-heading,button.list-group-item .list-group-item-heading{color:#333}a.list-group-item:focus,a.list-group-item:hover,button.list-group-item:focus,button.list-group-item:hover{color:#555;text-decoration:none;background-color:#f5f5f5}button.list-group-item{width:100%;text-align:left}.list-group-item.disabled,.list-group-item.disabled:focus,.list-group-item.disabled:hover{color:#777;cursor:not-allowed;background-color:#eee}.list-group-item.disabled .list-group-item-heading,.list-group-item.disabled:focus .list-group-item-heading,.list-group-item.disabled:hover .list-group-item-heading{color:inherit}.list-group-item.disabled .list-group-item-text,.list-group-item.disabled:focus .list-group-item-text,.list-group-item.disabled:hover .list-group-item-text{color:#777}.list-group-item.active,.list-group-item.active:focus,.list-group-item.active:hover{z-index:2;color:#fff;background-color:#337ab7;border-color:#337ab7}.list-group-item.active .list-group-item-heading,.list-group-item.active .list-group-item-heading>.small,.list-group-item.active .list-group-item-heading>small,.list-group-item.active:focus .list-group-item-heading,.list-group-item.active:focus .list-group-item-heading>.small,.list-group-item.active:focus .list-group-item-heading>small,.list-group-item.active:hover .list-group-item-heading,.list-group-item.active:hover .list-group-item-heading>.small,.list-group-item.active:hover .list-group-item-heading>small{color:inherit}.list-group-item.active .list-group-item-text,.list-group-item.active:focus .list-group-item-text,.list-group-item.active:hover .list-group-item-text{color:#c7ddef}.list-group-item-success{color:#3c763d;background-color:#dff0d8}a.list-group-item-success,button.list-group-item-success{color:#3c763d}a.list-group-item-success .list-group-item-heading,button.list-group-item-success .list-group-item-heading{color:inherit}a.list-group-item-success:focus,a.list-group-item-success:hover,button.list-group-item-success:focus,button.list-group-item-success:hover{color:#3c763d;background-color:#d0e9c6}a.list-group-item-success.active,a.list-group-item-success.active:focus,a.list-group-item-success.active:hover,button.list-group-item-success.active,button.list-group-item-success.active:focus,button.list-group-item-success.active:hover{color:#fff;background-color:#3c763d;border-color:#3c763d}.list-group-item-info{color:#31708f;background-color:#d9edf7}a.list-group-item-info,button.list-group-item-info{color:#31708f}a.list-group-item-info .list-group-item-heading,button.list-group-item-info .list-group-item-heading{color:inherit}a.list-group-item-info:focus,a.list-group-item-info:hover,button.list-group-item-info:focus,button.list-group-item-info:hover{color:#31708f;background-color:#c4e3f3}a.list-group-item-info.active,a.list-group-item-info.active:focus,a.list-group-item-info.active:hover,button.list-group-item-info.active,button.list-group-item-info.active:focus,button.list-group-item-info.active:hover{color:#fff;background-color:#31708f;border-color:#31708f}.list-group-item-warning{color:#8a6d3b;background-color:#fcf8e3}a.list-group-item-warning,button.list-group-item-warning{color:#8a6d3b}a.list-group-item-warning .list-group-item-heading,button.list-group-item-warning .list-group-item-heading{color:inherit}a.list-group-item-warning:focus,a.list-group-item-warning:hover,button.list-group-item-warning:focus,button.list-group-item-warning:hover{color:#8a6d3b;background-color:#faf2cc}a.list-group-item-warning.active,a.list-group-item-warning.active:focus,a.list-group-item-warning.active:hover,button.list-group-item-warning.active,button.list-group-item-warning.active:focus,button.list-group-item-warning.active:hover{color:#fff;background-color:#8a6d3b;border-color:#8a6d3b}.list-group-item-danger{color:#a94442;background-color:#f2dede}a.list-group-item-danger,button.list-group-item-danger{color:#a94442}a.list-group-item-danger .list-group-item-heading,button.list-group-item-danger .list-group-item-heading{color:inherit}a.list-group-item-danger:focus,a.list-group-item-danger:hover,button.list-group-item-danger:focus,button.list-group-item-danger:hover{color:#a94442;background-color:#ebcccc}a.list-group-item-danger.active,a.list-group-item-danger.active:focus,a.list-group-item-danger.active:hover,button.list-group-item-danger.active,button.list-group-item-danger.active:focus,button.list-group-item-danger.active:hover{color:#fff;background-color:#a94442;border-color:#a94442}.list-group-item-heading{margin-top:0;margin-bottom:5px}.list-group-item-text{margin-bottom:0;line-height:1.3}.panel{margin-bottom:20px;background-color:#fff;border:1px solid transparent;border-radius:4px;-webkit-box-shadow:0 1px 1px rgba(0,0,0,.05);box-shadow:0 1px 1px rgba(0,0,0,.05)}.panel-body{padding:15px}.panel-heading{padding:10px 15px;border-bottom:1px solid transparent;border-top-left-radius:3px;border-top-right-radius:3px}.panel-heading>.dropdown .dropdown-toggle{color:inherit}.panel-title{margin-top:0;margin-bottom:0;font-size:16px;color:inherit}.panel-title>.small,.panel-title>.small>a,.panel-title>a,.panel-title>small,.panel-title>small>a{color:inherit}.panel-footer{padding:10px 15px;background-color:#f5f5f5;border-top:1px solid #ddd;border-bottom-right-radius:3px;border-bottom-left-radius:3px}.panel>.list-group,.panel>.panel-collapse>.list-group{margin-bottom:0}.panel>.list-group .list-group-item,.panel>.panel-collapse>.list-group .list-group-item{border-width:1px 0;border-radius:0}.panel>.list-group:first-child .list-group-item:first-child,.panel>.panel-collapse>.list-group:first-child .list-group-item:first-child{border-top:0;border-top-left-radius:3px;border-top-right-radius:3px}.panel>.list-group:last-child .list-group-item:last-child,.panel>.panel-collapse>.list-group:last-child .list-group-item:last-child{border-bottom:0;border-bottom-right-radius:3px;border-bottom-left-radius:3px}.panel>.panel-heading+.panel-collapse>.list-group .list-group-item:first-child{border-top-left-radius:0;border-top-right-radius:0}.panel-heading+.list-group .list-group-item:first-child{border-top-width:0}.list-group+.panel-footer{border-top-width:0}.panel>.panel-collapse>.table,.panel>.table,.panel>.table-responsive>.table{margin-bottom:0}.panel>.panel-collapse>.table caption,.panel>.table caption,.panel>.table-responsive>.table caption{padding-right:15px;padding-left:15px}.panel>.table-responsive:first-child>.table:first-child,.panel>.table:first-child{border-top-left-radius:3px;border-top-right-radius:3px}.panel>.table-responsive:first-child>.table:first-child>tbody:first-child>tr:first-child,.panel>.table-responsive:first-child>.table:first-child>thead:first-child>tr:first-child,.panel>.table:first-child>tbody:first-child>tr:first-child,.panel>.table:first-child>thead:first-child>tr:first-child{border-top-left-radius:3px;border-top-right-radius:3px}.panel>.table-responsive:first-child>.table:first-child>tbody:first-child>tr:first-child td:first-child,.panel>.table-responsive:first-child>.table:first-child>tbody:first-child>tr:first-child th:first-child,.panel>.table-responsive:first-child>.table:first-child>thead:first-child>tr:first-child td:first-child,.panel>.table-responsive:first-child>.table:first-child>thead:first-child>tr:first-child th:first-child,.panel>.table:first-child>tbody:first-child>tr:first-child td:first-child,.panel>.table:first-child>tbody:first-child>tr:first-child th:first-child,.panel>.table:first-child>thead:first-child>tr:first-child td:first-child,.panel>.table:first-child>thead:first-child>tr:first-child th:first-child{border-top-left-radius:3px}.panel>.table-responsive:first-child>.table:first-child>tbody:first-child>tr:first-child td:last-child,.panel>.table-responsive:first-child>.table:first-child>tbody:first-child>tr:first-child th:last-child,.panel>.table-responsive:first-child>.table:first-child>thead:first-child>tr:first-child td:last-child,.panel>.table-responsive:first-child>.table:first-child>thead:first-child>tr:first-child th:last-child,.panel>.table:first-child>tbody:first-child>tr:first-child td:last-child,.panel>.table:first-child>tbody:first-child>tr:first-child th:last-child,.panel>.table:first-child>thead:first-child>tr:first-child td:last-child,.panel>.table:first-child>thead:first-child>tr:first-child th:last-child{border-top-right-radius:3px}.panel>.table-responsive:last-child>.table:last-child,.panel>.table:last-child{border-bottom-right-radius:3px;border-bottom-left-radius:3px}.panel>.table-responsive:last-child>.table:last-child>tbody:last-child>tr:last-child,.panel>.table-responsive:last-child>.table:last-child>tfoot:last-child>tr:last-child,.panel>.table:last-child>tbody:last-child>tr:last-child,.panel>.table:last-child>tfoot:last-child>tr:last-child{border-bottom-right-radius:3px;border-bottom-left-radius:3px}.panel>.table-responsive:last-child>.table:last-child>tbody:last-child>tr:last-child td:first-child,.panel>.table-responsive:last-child>.table:last-child>tbody:last-child>tr:last-child th:first-child,.panel>.table-responsive:last-child>.table:last-child>tfoot:last-child>tr:last-child td:first-child,.panel>.table-responsive:last-child>.table:last-child>tfoot:last-child>tr:last-child th:first-child,.panel>.table:last-child>tbody:last-child>tr:last-child td:first-child,.panel>.table:last-child>tbody:last-child>tr:last-child th:first-child,.panel>.table:last-child>tfoot:last-child>tr:last-child td:first-child,.panel>.table:last-child>tfoot:last-child>tr:last-child th:first-child{border-bottom-left-radius:3px}.panel>.table-responsive:last-child>.table:last-child>tbody:last-child>tr:last-child td:last-child,.panel>.table-responsive:last-child>.table:last-child>tbody:last-child>tr:last-child th:last-child,.panel>.table-responsive:last-child>.table:last-child>tfoot:last-child>tr:last-child td:last-child,.panel>.table-responsive:last-child>.table:last-child>tfoot:last-child>tr:last-child th:last-child,.panel>.table:last-child>tbody:last-child>tr:last-child td:last-child,.panel>.table:last-child>tbody:last-child>tr:last-child th:last-child,.panel>.table:last-child>tfoot:last-child>tr:last-child td:last-child,.panel>.table:last-child>tfoot:last-child>tr:last-child th:last-child{border-bottom-right-radius:3px}.panel>.panel-body+.table,.panel>.panel-body+.table-responsive,.panel>.table+.panel-body,.panel>.table-responsive+.panel-body{border-top:1px solid #ddd}.panel>.table>tbody:first-child>tr:first-child td,.panel>.table>tbody:first-child>tr:first-child th{border-top:0}.panel>.table-bordered,.panel>.table-responsive>.table-bordered{border:0}.panel>.table-bordered>tbody>tr>td:first-child,.panel>.table-bordered>tbody>tr>th:first-child,.panel>.table-bordered>tfoot>tr>td:first-child,.panel>.table-bordered>tfoot>tr>th:first-child,.panel>.table-bordered>thead>tr>td:first-child,.panel>.table-bordered>thead>tr>th:first-child,.panel>.table-responsive>.table-bordered>tbody>tr>td:first-child,.panel>.table-responsive>.table-bordered>tbody>tr>th:first-child,.panel>.table-responsive>.table-bordered>tfoot>tr>td:first-child,.panel>.table-responsive>.table-bordered>tfoot>tr>th:first-child,.panel>.table-responsive>.table-bordered>thead>tr>td:first-child,.panel>.table-responsive>.table-bordered>thead>tr>th:first-child{border-left:0}.panel>.table-bordered>tbody>tr>td:last-child,.panel>.table-bordered>tbody>tr>th:last-child,.panel>.table-bordered>tfoot>tr>td:last-child,.panel>.table-bordered>tfoot>tr>th:last-child,.panel>.table-bordered>thead>tr>td:last-child,.panel>.table-bordered>thead>tr>th:last-child,.panel>.table-responsive>.table-bordered>tbody>tr>td:last-child,.panel>.table-responsive>.table-bordered>tbody>tr>th:last-child,.panel>.table-responsive>.table-bordered>tfoot>tr>td:last-child,.panel>.table-responsive>.table-bordered>tfoot>tr>th:last-child,.panel>.table-responsive>.table-bordered>thead>tr>td:last-child,.panel>.table-responsive>.table-bordered>thead>tr>th:last-child{border-right:0}.panel>.table-bordered>tbody>tr:first-child>td,.panel>.table-bordered>tbody>tr:first-child>th,.panel>.table-bordered>thead>tr:first-child>td,.panel>.table-bordered>thead>tr:first-child>th,.panel>.table-responsive>.table-bordered>tbody>tr:first-child>td,.panel>.table-responsive>.table-bordered>tbody>tr:first-child>th,.panel>.table-responsive>.table-bordered>thead>tr:first-child>td,.panel>.table-responsive>.table-bordered>thead>tr:first-child>th{border-bottom:0}.panel>.table-bordered>tbody>tr:last-child>td,.panel>.table-bordered>tbody>tr:last-child>th,.panel>.table-bordered>tfoot>tr:last-child>td,.panel>.table-bordered>tfoot>tr:last-child>th,.panel>.table-responsive>.table-bordered>tbody>tr:last-child>td,.panel>.table-responsive>.table-bordered>tbody>tr:last-child>th,.panel>.table-responsive>.table-bordered>tfoot>tr:last-child>td,.panel>.table-responsive>.table-bordered>tfoot>tr:last-child>th{border-bottom:0}.panel>.table-responsive{margin-bottom:0;border:0}.panel-group{margin-bottom:20px}.panel-group .panel{margin-bottom:0;border-radius:4px}.panel-group .panel+.panel{margin-top:5px}.panel-group .panel-heading{border-bottom:0}.panel-group .panel-heading+.panel-collapse>.list-group,.panel-group .panel-heading+.panel-collapse>.panel-body{border-top:1px solid #ddd}.panel-group .panel-footer{border-top:0}.panel-group .panel-footer+.panel-collapse .panel-body{border-bottom:1px solid #ddd}.panel-default{border-color:#ddd}.panel-default>.panel-heading{color:#333;background-color:#f5f5f5;border-color:#ddd}.panel-default>.panel-heading+.panel-collapse>.panel-body{border-top-color:#ddd}.panel-default>.panel-heading .badge{color:#f5f5f5;background-color:#333}.panel-default>.panel-footer+.panel-collapse>.panel-body{border-bottom-color:#ddd}.panel-primary{border-color:#337ab7}.panel-primary>.panel-heading{color:#fff;background-color:#337ab7;border-color:#337ab7}.panel-primary>.panel-heading+.panel-collapse>.panel-body{border-top-color:#337ab7}.panel-primary>.panel-heading .badge{color:#337ab7;background-color:#fff}.panel-primary>.panel-footer+.panel-collapse>.panel-body{border-bottom-color:#337ab7}.panel-success{border-color:#d6e9c6}.panel-success>.panel-heading{color:#3c763d;background-color:#dff0d8;border-color:#d6e9c6}.panel-success>.panel-heading+.panel-collapse>.panel-body{border-top-color:#d6e9c6}.panel-success>.panel-heading .badge{color:#dff0d8;background-color:#3c763d}.panel-success>.panel-footer+.panel-collapse>.panel-body{border-bottom-color:#d6e9c6}.panel-info{border-color:#bce8f1}.panel-info>.panel-heading{color:#31708f;background-color:#d9edf7;border-color:#bce8f1}.panel-info>.panel-heading+.panel-collapse>.panel-body{border-top-color:#bce8f1}.panel-info>.panel-heading .badge{color:#d9edf7;background-color:#31708f}.panel-info>.panel-footer+.panel-collapse>.panel-body{border-bottom-color:#bce8f1}.panel-warning{border-color:#faebcc}.panel-warning>.panel-heading{color:#8a6d3b;background-color:#fcf8e3;border-color:#faebcc}.panel-warning>.panel-heading+.panel-collapse>.panel-body{border-top-color:#faebcc}.panel-warning>.panel-heading .badge{color:#fcf8e3;background-color:#8a6d3b}.panel-warning>.panel-footer+.panel-collapse>.panel-body{border-bottom-color:#faebcc}.panel-danger{border-color:#ebccd1}.panel-danger>.panel-heading{color:#a94442;background-color:#f2dede;border-color:#ebccd1}.panel-danger>.panel-heading+.panel-collapse>.panel-body{border-top-color:#ebccd1}.panel-danger>.panel-heading .badge{color:#f2dede;background-color:#a94442}.panel-danger>.panel-footer+.panel-collapse>.panel-body{border-bottom-color:#ebccd1}.embed-responsive{position:relative;display:block;height:0;padding:0;overflow:hidden}.embed-responsive .embed-responsive-item,.embed-responsive embed,.embed-responsive iframe,.embed-responsive object,.embed-responsive video{position:absolute;top:0;bottom:0;left:0;width:100%;height:100%;border:0}.embed-responsive-16by9{padding-bottom:56.25%}.embed-responsive-4by3{padding-bottom:75%}.well{min-height:20px;padding:19px;margin-bottom:20px;background-color:#f5f5f5;border:1px solid #e3e3e3;border-radius:4px;-webkit-box-shadow:inset 0 1px 1px rgba(0,0,0,.05);box-shadow:inset 0 1px 1px rgba(0,0,0,.05)}.well blockquote{border-color:#ddd;border-color:rgba(0,0,0,.15)}.well-lg{padding:24px;border-radius:6px}.well-sm{padding:9px;border-radius:3px}.close{float:right;font-size:21px;font-weight:700;line-height:1;color:#000;text-shadow:0 1px 0 #fff;filter:alpha(opacity=20);opacity:.2}.close:focus,.close:hover{color:#000;text-decoration:none;cursor:pointer;filter:alpha(opacity=50);opacity:.5}button.close{-webkit-appearance:none;padding:0;cursor:pointer;background:0 0;border:0}.modal-open{overflow:hidden}.modal{position:fixed;top:0;right:0;bottom:0;left:0;z-index:1050;display:none;overflow:hidden;-webkit-overflow-scrolling:touch;outline:0}.modal.fade .modal-dialog{-webkit-transition:-webkit-transform .3s ease-out;-o-transition:-o-transform .3s ease-out;transition:transform .3s ease-out;-webkit-transform:translate(0,-25%);-ms-transform:translate(0,-25%);-o-transform:translate(0,-25%);transform:translate(0,-25%)}.modal.in .modal-dialog{-webkit-transform:translate(0,0);-ms-transform:translate(0,0);-o-transform:translate(0,0);transform:translate(0,0)}.modal-open .modal{overflow-x:hidden;overflow-y:auto}.modal-dialog{position:relative;width:auto;margin:10px}.modal-content{position:relative;background-color:#fff;-webkit-background-clip:padding-box;background-clip:padding-box;border:1px solid #999;border:1px solid rgba(0,0,0,.2);border-radius:6px;outline:0;-webkit-box-shadow:0 3px 9px rgba(0,0,0,.5);box-shadow:0 3px 9px rgba(0,0,0,.5)}.modal-backdrop{position:fixed;top:0;right:0;bottom:0;left:0;z-index:1040;background-color:#000}.modal-backdrop.fade{filter:alpha(opacity=0);opacity:0}.modal-backdrop.in{filter:alpha(opacity=50);opacity:.5}.modal-header{padding:15px;border-bottom:1px solid #e5e5e5}.modal-header .close{margin-top:-2px}.modal-title{margin:0;line-height:1.42857143}.modal-body{position:relative;padding:15px}.modal-footer{padding:15px;text-align:right;border-top:1px solid #e5e5e5}.modal-footer .btn+.btn{margin-bottom:0;margin-left:5px}.modal-footer .btn-group .btn+.btn{margin-left:-1px}.modal-footer .btn-block+.btn-block{margin-left:0}.modal-scrollbar-measure{position:absolute;top:-9999px;width:50px;height:50px;overflow:scroll}@media (min-width:768px){.modal-dialog{width:600px;margin:30px auto}.modal-content{-webkit-box-shadow:0 5px 15px rgba(0,0,0,.5);box-shadow:0 5px 15px rgba(0,0,0,.5)}.modal-sm{width:300px}}@media (min-width:992px){.modal-lg{width:900px}}.tooltip{position:absolute;z-index:1070;display:block;font-family:\"Helvetica Neue\",Helvetica,Arial,sans-serif;font-size:12px;font-style:normal;font-weight:400;line-height:1.42857143;text-align:left;text-align:start;text-decoration:none;text-shadow:none;text-transform:none;letter-spacing:normal;word-break:normal;word-spacing:normal;word-wrap:normal;white-space:normal;filter:alpha(opacity=0);opacity:0;line-break:auto}.tooltip.in{filter:alpha(opacity=90);opacity:.9}.tooltip.top{padding:5px 0;margin-top:-3px}.tooltip.right{padding:0 5px;margin-left:3px}.tooltip.bottom{padding:5px 0;margin-top:3px}.tooltip.left{padding:0 5px;margin-left:-3px}.tooltip-inner{max-width:200px;padding:3px 8px;color:#fff;text-align:center;background-color:#000;border-radius:4px}.tooltip-arrow{position:absolute;width:0;height:0;border-color:transparent;border-style:solid}.tooltip.top .tooltip-arrow{bottom:0;left:50%;margin-left:-5px;border-width:5px 5px 0;border-top-color:#000}.tooltip.top-left .tooltip-arrow{right:5px;bottom:0;margin-bottom:-5px;border-width:5px 5px 0;border-top-color:#000}.tooltip.top-right .tooltip-arrow{bottom:0;left:5px;margin-bottom:-5px;border-width:5px 5px 0;border-top-color:#000}.tooltip.right .tooltip-arrow{top:50%;left:0;margin-top:-5px;border-width:5px 5px 5px 0;border-right-color:#000}.tooltip.left .tooltip-arrow{top:50%;right:0;margin-top:-5px;border-width:5px 0 5px 5px;border-left-color:#000}.tooltip.bottom .tooltip-arrow{top:0;left:50%;margin-left:-5px;border-width:0 5px 5px;border-bottom-color:#000}.tooltip.bottom-left .tooltip-arrow{top:0;right:5px;margin-top:-5px;border-width:0 5px 5px;border-bottom-color:#000}.tooltip.bottom-right .tooltip-arrow{top:0;left:5px;margin-top:-5px;border-width:0 5px 5px;border-bottom-color:#000}.popover{position:absolute;top:0;left:0;z-index:1060;display:none;max-width:276px;padding:1px;font-family:\"Helvetica Neue\",Helvetica,Arial,sans-serif;font-size:14px;font-style:normal;font-weight:400;line-height:1.42857143;text-align:left;text-align:start;text-decoration:none;text-shadow:none;text-transform:none;letter-spacing:normal;word-break:normal;word-spacing:normal;word-wrap:normal;white-space:normal;background-color:#fff;-webkit-background-clip:padding-box;background-clip:padding-box;border:1px solid #ccc;border:1px solid rgba(0,0,0,.2);border-radius:6px;-webkit-box-shadow:0 5px 10px rgba(0,0,0,.2);box-shadow:0 5px 10px rgba(0,0,0,.2);line-break:auto}.popover.top{margin-top:-10px}.popover.right{margin-left:10px}.popover.bottom{margin-top:10px}.popover.left{margin-left:-10px}.popover-title{padding:8px 14px;margin:0;font-size:14px;background-color:#f7f7f7;border-bottom:1px solid #ebebeb;border-radius:5px 5px 0 0}.popover-content{padding:9px 14px}.popover>.arrow,.popover>.arrow:after{position:absolute;display:block;width:0;height:0;border-color:transparent;border-style:solid}.popover>.arrow{border-width:11px}.popover>.arrow:after{content:\"\";border-width:10px}.popover.top>.arrow{bottom:-11px;left:50%;margin-left:-11px;border-top-color:#999;border-top-color:rgba(0,0,0,.25);border-bottom-width:0}.popover.top>.arrow:after{bottom:1px;margin-left:-10px;content:\" \";border-top-color:#fff;border-bottom-width:0}.popover.right>.arrow{top:50%;left:-11px;margin-top:-11px;border-right-color:#999;border-right-color:rgba(0,0,0,.25);border-left-width:0}.popover.right>.arrow:after{bottom:-10px;left:1px;content:\" \";border-right-color:#fff;border-left-width:0}.popover.bottom>.arrow{top:-11px;left:50%;margin-left:-11px;border-top-width:0;border-bottom-color:#999;border-bottom-color:rgba(0,0,0,.25)}.popover.bottom>.arrow:after{top:1px;margin-left:-10px;content:\" \";border-top-width:0;border-bottom-color:#fff}.popover.left>.arrow{top:50%;right:-11px;margin-top:-11px;border-right-width:0;border-left-color:#999;border-left-color:rgba(0,0,0,.25)}.popover.left>.arrow:after{right:1px;bottom:-10px;content:\" \";border-right-width:0;border-left-color:#fff}.carousel{position:relative}.carousel-inner{position:relative;width:100%;overflow:hidden}.carousel-inner>.item{position:relative;display:none;-webkit-transition:.6s ease-in-out left;-o-transition:.6s ease-in-out left;transition:.6s ease-in-out left}.carousel-inner>.item>a>img,.carousel-inner>.item>img{line-height:1}@media all and (transform-3d),(-webkit-transform-3d){.carousel-inner>.item{-webkit-transition:-webkit-transform .6s ease-in-out;-o-transition:-o-transform .6s ease-in-out;transition:transform .6s ease-in-out;-webkit-backface-visibility:hidden;backface-visibility:hidden;-webkit-perspective:1000px;perspective:1000px}.carousel-inner>.item.active.right,.carousel-inner>.item.next{left:0;-webkit-transform:translate3d(100%,0,0);transform:translate3d(100%,0,0)}.carousel-inner>.item.active.left,.carousel-inner>.item.prev{left:0;-webkit-transform:translate3d(-100%,0,0);transform:translate3d(-100%,0,0)}.carousel-inner>.item.active,.carousel-inner>.item.next.left,.carousel-inner>.item.prev.right{left:0;-webkit-transform:translate3d(0,0,0);transform:translate3d(0,0,0)}}.carousel-inner>.active,.carousel-inner>.next,.carousel-inner>.prev{display:block}.carousel-inner>.active{left:0}.carousel-inner>.next,.carousel-inner>.prev{position:absolute;top:0;width:100%}.carousel-inner>.next{left:100%}.carousel-inner>.prev{left:-100%}.carousel-inner>.next.left,.carousel-inner>.prev.right{left:0}.carousel-inner>.active.left{left:-100%}.carousel-inner>.active.right{left:100%}.carousel-control{position:absolute;top:0;bottom:0;left:0;width:15%;font-size:20px;color:#fff;text-align:center;text-shadow:0 1px 2px rgba(0,0,0,.6);background-color:rgba(0,0,0,0);filter:alpha(opacity=50);opacity:.5}.carousel-control.left{background-image:-webkit-linear-gradient(left,rgba(0,0,0,.5) 0,rgba(0,0,0,.0001) 100%);background-image:-o-linear-gradient(left,rgba(0,0,0,.5) 0,rgba(0,0,0,.0001) 100%);background-image:-webkit-gradient(linear,left top,right top,from(rgba(0,0,0,.5)),to(rgba(0,0,0,.0001)));background-image:linear-gradient(to right,rgba(0,0,0,.5) 0,rgba(0,0,0,.0001) 100%);filter:progid:DXImageTransform.Microsoft.gradient(startColorstr='#80000000', endColorstr='#00000000', GradientType=1);background-repeat:repeat-x}.carousel-control.right{right:0;left:auto;background-image:-webkit-linear-gradient(left,rgba(0,0,0,.0001) 0,rgba(0,0,0,.5) 100%);background-image:-o-linear-gradient(left,rgba(0,0,0,.0001) 0,rgba(0,0,0,.5) 100%);background-image:-webkit-gradient(linear,left top,right top,from(rgba(0,0,0,.0001)),to(rgba(0,0,0,.5)));background-image:linear-gradient(to right,rgba(0,0,0,.0001) 0,rgba(0,0,0,.5) 100%);filter:progid:DXImageTransform.Microsoft.gradient(startColorstr='#00000000', endColorstr='#80000000', GradientType=1);background-repeat:repeat-x}.carousel-control:focus,.carousel-control:hover{color:#fff;text-decoration:none;filter:alpha(opacity=90);outline:0;opacity:.9}.carousel-control .glyphicon-chevron-left,.carousel-control .glyphicon-chevron-right,.carousel-control .icon-next,.carousel-control .icon-prev{position:absolute;top:50%;z-index:5;display:inline-block;margin-top:-10px}.carousel-control .glyphicon-chevron-left,.carousel-control .icon-prev{left:50%;margin-left:-10px}.carousel-control .glyphicon-chevron-right,.carousel-control .icon-next{right:50%;margin-right:-10px}.carousel-control .icon-next,.carousel-control .icon-prev{width:20px;height:20px;font-family:serif;line-height:1}.carousel-control .icon-prev:before{content:'\\2039'}.carousel-control .icon-next:before{content:'\\203A'}.carousel-indicators{position:absolute;bottom:10px;left:50%;z-index:15;width:60%;padding-left:0;margin-left:-30%;text-align:center;list-style:none}.carousel-indicators li{display:inline-block;width:10px;height:10px;margin:1px;text-indent:-999px;cursor:pointer;background-color:#000\\9;background-color:rgba(0,0,0,0);border:1px solid #fff;border-radius:10px}.carousel-indicators .active{width:12px;height:12px;margin:0;background-color:#fff}.carousel-caption{position:absolute;right:15%;bottom:20px;left:15%;z-index:10;padding-top:20px;padding-bottom:20px;color:#fff;text-align:center;text-shadow:0 1px 2px rgba(0,0,0,.6)}.carousel-caption .btn{text-shadow:none}@media screen and (min-width:768px){.carousel-control .glyphicon-chevron-left,.carousel-control .glyphicon-chevron-right,.carousel-control .icon-next,.carousel-control .icon-prev{width:30px;height:30px;margin-top:-10px;font-size:30px}.carousel-control .glyphicon-chevron-left,.carousel-control .icon-prev{margin-left:-10px}.carousel-control .glyphicon-chevron-right,.carousel-control .icon-next{margin-right:-10px}.carousel-caption{right:20%;left:20%;padding-bottom:30px}.carousel-indicators{bottom:20px}}.btn-group-vertical>.btn-group:after,.btn-group-vertical>.btn-group:before,.btn-toolbar:after,.btn-toolbar:before,.clearfix:after,.clearfix:before,.container-fluid:after,.container-fluid:before,.container:after,.container:before,.dl-horizontal dd:after,.dl-horizontal dd:before,.form-horizontal .form-group:after,.form-horizontal .form-group:before,.modal-footer:after,.modal-footer:before,.modal-header:after,.modal-header:before,.nav:after,.nav:before,.navbar-collapse:after,.navbar-collapse:before,.navbar-header:after,.navbar-header:before,.navbar:after,.navbar:before,.pager:after,.pager:before,.panel-body:after,.panel-body:before,.row:after,.row:before{display:table;content:\" \"}.btn-group-vertical>.btn-group:after,.btn-toolbar:after,.clearfix:after,.container-fluid:after,.container:after,.dl-horizontal dd:after,.form-horizontal .form-group:after,.modal-footer:after,.modal-header:after,.nav:after,.navbar-collapse:after,.navbar-header:after,.navbar:after,.pager:after,.panel-body:after,.row:after{clear:both}.center-block{display:block;margin-right:auto;margin-left:auto}.pull-right{float:right!important}.pull-left{float:left!important}.hide{display:none!important}.show{display:block!important}.invisible{visibility:hidden}.text-hide{font:0/0 a;color:transparent;text-shadow:none;background-color:transparent;border:0}.hidden{display:none!important}.affix{position:fixed}@-ms-viewport{width:device-width}.visible-lg,.visible-md,.visible-sm,.visible-xs{display:none!important}.visible-lg-block,.visible-lg-inline,.visible-lg-inline-block,.visible-md-block,.visible-md-inline,.visible-md-inline-block,.visible-sm-block,.visible-sm-inline,.visible-sm-inline-block,.visible-xs-block,.visible-xs-inline,.visible-xs-inline-block{display:none!important}@media (max-width:767px){.visible-xs{display:block!important}table.visible-xs{display:table!important}tr.visible-xs{display:table-row!important}td.visible-xs,th.visible-xs{display:table-cell!important}}@media (max-width:767px){.visible-xs-block{display:block!important}}@media (max-width:767px){.visible-xs-inline{display:inline!important}}@media (max-width:767px){.visible-xs-inline-block{display:inline-block!important}}@media (min-width:768px) and (max-width:991px){.visible-sm{display:block!important}table.visible-sm{display:table!important}tr.visible-sm{display:table-row!important}td.visible-sm,th.visible-sm{display:table-cell!important}}@media (min-width:768px) and (max-width:991px){.visible-sm-block{display:block!important}}@media (min-width:768px) and (max-width:991px){.visible-sm-inline{display:inline!important}}@media (min-width:768px) and (max-width:991px){.visible-sm-inline-block{display:inline-block!important}}@media (min-width:992px) and (max-width:1199px){.visible-md{display:block!important}table.visible-md{display:table!important}tr.visible-md{display:table-row!important}td.visible-md,th.visible-md{display:table-cell!important}}@media (min-width:992px) and (max-width:1199px){.visible-md-block{display:block!important}}@media (min-width:992px) and (max-width:1199px){.visible-md-inline{display:inline!important}}@media (min-width:992px) and (max-width:1199px){.visible-md-inline-block{display:inline-block!important}}@media (min-width:1200px){.visible-lg{display:block!important}table.visible-lg{display:table!important}tr.visible-lg{display:table-row!important}td.visible-lg,th.visible-lg{display:table-cell!important}}@media (min-width:1200px){.visible-lg-block{display:block!important}}@media (min-width:1200px){.visible-lg-inline{display:inline!important}}@media (min-width:1200px){.visible-lg-inline-block{display:inline-block!important}}@media (max-width:767px){.hidden-xs{display:none!important}}@media (min-width:768px) and (max-width:991px){.hidden-sm{display:none!important}}@media (min-width:992px) and (max-width:1199px){.hidden-md{display:none!important}}@media (min-width:1200px){.hidden-lg{display:none!important}}.visible-print{display:none!important}@media print{.visible-print{display:block!important}table.visible-print{display:table!important}tr.visible-print{display:table-row!important}td.visible-print,th.visible-print{display:table-cell!important}}.visible-print-block{display:none!important}@media print{.visible-print-block{display:block!important}}.visible-print-inline{display:none!important}@media print{.visible-print-inline{display:inline!important}}.visible-print-inline-block{display:none!important}@media print{.visible-print-inline-block{display:inline-block!important}}@media print{.hidden-print{display:none!important}}\n/*# sourceMappingURL=bootstrap.min.css.map */", ""]);

	// exports


/***/ },
/* 4 */
/***/ function(module, exports) {

	/*
		MIT License http://www.opensource.org/licenses/mit-license.php
		Author Tobias Koppers @sokra
	*/
	// css base code, injected by the css-loader
	module.exports = function() {
		var list = [];

		// return the list of modules as css string
		list.toString = function toString() {
			var result = [];
			for(var i = 0; i < this.length; i++) {
				var item = this[i];
				if(item[2]) {
					result.push("@media " + item[2] + "{" + item[1] + "}");
				} else {
					result.push(item[1]);
				}
			}
			return result.join("");
		};

		// import a list of modules into the list
		list.i = function(modules, mediaQuery) {
			if(typeof modules === "string")
				modules = [[null, modules, ""]];
			var alreadyImportedModules = {};
			for(var i = 0; i < this.length; i++) {
				var id = this[i][0];
				if(typeof id === "number")
					alreadyImportedModules[id] = true;
			}
			for(i = 0; i < modules.length; i++) {
				var item = modules[i];
				// skip already imported module
				// this implementation is not 100% perfect for weird media query combinations
				//  when a module is imported multiple times with different media queries.
				//  I hope this will never occur (Hey this way we have smaller bundles)
				if(typeof item[0] !== "number" || !alreadyImportedModules[item[0]]) {
					if(mediaQuery && !item[2]) {
						item[2] = mediaQuery;
					} else if(mediaQuery) {
						item[2] = "(" + item[2] + ") and (" + mediaQuery + ")";
					}
					list.push(item);
				}
			}
		};
		return list;
	};


/***/ },
/* 5 */
/***/ function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__.p + "f4769f9bdb7466be65088239c12046d1.eot";

/***/ },
/* 6 */
/***/ function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__.p + "448c34a56d699c29117adc64c43affeb.woff2";

/***/ },
/* 7 */
/***/ function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__.p + "fa2772327f55d8198301fdb8bcfc8158.woff";

/***/ },
/* 8 */
/***/ function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__.p + "e18bbf611f2a2e43afc071aa2f4e1512.ttf";

/***/ },
/* 9 */
/***/ function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__.p + "89889688147bd7575d6327160d64e760.svg";

/***/ },
/* 10 */
/***/ function(module, exports, __webpack_require__) {

	/*
		MIT License http://www.opensource.org/licenses/mit-license.php
		Author Tobias Koppers @sokra
	*/
	var stylesInDom = {},
		memoize = function(fn) {
			var memo;
			return function () {
				if (typeof memo === "undefined") memo = fn.apply(this, arguments);
				return memo;
			};
		},
		isOldIE = memoize(function() {
			return /msie [6-9]\b/.test(window.navigator.userAgent.toLowerCase());
		}),
		getHeadElement = memoize(function () {
			return document.head || document.getElementsByTagName("head")[0];
		}),
		singletonElement = null,
		singletonCounter = 0,
		styleElementsInsertedAtTop = [];

	module.exports = function(list, options) {
		if(false) {
			if(typeof document !== "object") throw new Error("The style-loader cannot be used in a non-browser environment");
		}

		options = options || {};
		// Force single-tag solution on IE6-9, which has a hard limit on the # of <style>
		// tags it will allow on a page
		if (typeof options.singleton === "undefined") options.singleton = isOldIE();

		// By default, add <style> tags to the bottom of <head>.
		if (typeof options.insertAt === "undefined") options.insertAt = "bottom";

		var styles = listToStyles(list);
		addStylesToDom(styles, options);

		return function update(newList) {
			var mayRemove = [];
			for(var i = 0; i < styles.length; i++) {
				var item = styles[i];
				var domStyle = stylesInDom[item.id];
				domStyle.refs--;
				mayRemove.push(domStyle);
			}
			if(newList) {
				var newStyles = listToStyles(newList);
				addStylesToDom(newStyles, options);
			}
			for(var i = 0; i < mayRemove.length; i++) {
				var domStyle = mayRemove[i];
				if(domStyle.refs === 0) {
					for(var j = 0; j < domStyle.parts.length; j++)
						domStyle.parts[j]();
					delete stylesInDom[domStyle.id];
				}
			}
		};
	}

	function addStylesToDom(styles, options) {
		for(var i = 0; i < styles.length; i++) {
			var item = styles[i];
			var domStyle = stylesInDom[item.id];
			if(domStyle) {
				domStyle.refs++;
				for(var j = 0; j < domStyle.parts.length; j++) {
					domStyle.parts[j](item.parts[j]);
				}
				for(; j < item.parts.length; j++) {
					domStyle.parts.push(addStyle(item.parts[j], options));
				}
			} else {
				var parts = [];
				for(var j = 0; j < item.parts.length; j++) {
					parts.push(addStyle(item.parts[j], options));
				}
				stylesInDom[item.id] = {id: item.id, refs: 1, parts: parts};
			}
		}
	}

	function listToStyles(list) {
		var styles = [];
		var newStyles = {};
		for(var i = 0; i < list.length; i++) {
			var item = list[i];
			var id = item[0];
			var css = item[1];
			var media = item[2];
			var sourceMap = item[3];
			var part = {css: css, media: media, sourceMap: sourceMap};
			if(!newStyles[id])
				styles.push(newStyles[id] = {id: id, parts: [part]});
			else
				newStyles[id].parts.push(part);
		}
		return styles;
	}

	function insertStyleElement(options, styleElement) {
		var head = getHeadElement();
		var lastStyleElementInsertedAtTop = styleElementsInsertedAtTop[styleElementsInsertedAtTop.length - 1];
		if (options.insertAt === "top") {
			if(!lastStyleElementInsertedAtTop) {
				head.insertBefore(styleElement, head.firstChild);
			} else if(lastStyleElementInsertedAtTop.nextSibling) {
				head.insertBefore(styleElement, lastStyleElementInsertedAtTop.nextSibling);
			} else {
				head.appendChild(styleElement);
			}
			styleElementsInsertedAtTop.push(styleElement);
		} else if (options.insertAt === "bottom") {
			head.appendChild(styleElement);
		} else {
			throw new Error("Invalid value for parameter 'insertAt'. Must be 'top' or 'bottom'.");
		}
	}

	function removeStyleElement(styleElement) {
		styleElement.parentNode.removeChild(styleElement);
		var idx = styleElementsInsertedAtTop.indexOf(styleElement);
		if(idx >= 0) {
			styleElementsInsertedAtTop.splice(idx, 1);
		}
	}

	function createStyleElement(options) {
		var styleElement = document.createElement("style");
		styleElement.type = "text/css";
		insertStyleElement(options, styleElement);
		return styleElement;
	}

	function createLinkElement(options) {
		var linkElement = document.createElement("link");
		linkElement.rel = "stylesheet";
		insertStyleElement(options, linkElement);
		return linkElement;
	}

	function addStyle(obj, options) {
		var styleElement, update, remove;

		if (options.singleton) {
			var styleIndex = singletonCounter++;
			styleElement = singletonElement || (singletonElement = createStyleElement(options));
			update = applyToSingletonTag.bind(null, styleElement, styleIndex, false);
			remove = applyToSingletonTag.bind(null, styleElement, styleIndex, true);
		} else if(obj.sourceMap &&
			typeof URL === "function" &&
			typeof URL.createObjectURL === "function" &&
			typeof URL.revokeObjectURL === "function" &&
			typeof Blob === "function" &&
			typeof btoa === "function") {
			styleElement = createLinkElement(options);
			update = updateLink.bind(null, styleElement);
			remove = function() {
				removeStyleElement(styleElement);
				if(styleElement.href)
					URL.revokeObjectURL(styleElement.href);
			};
		} else {
			styleElement = createStyleElement(options);
			update = applyToTag.bind(null, styleElement);
			remove = function() {
				removeStyleElement(styleElement);
			};
		}

		update(obj);

		return function updateStyle(newObj) {
			if(newObj) {
				if(newObj.css === obj.css && newObj.media === obj.media && newObj.sourceMap === obj.sourceMap)
					return;
				update(obj = newObj);
			} else {
				remove();
			}
		};
	}

	var replaceText = (function () {
		var textStore = [];

		return function (index, replacement) {
			textStore[index] = replacement;
			return textStore.filter(Boolean).join('\n');
		};
	})();

	function applyToSingletonTag(styleElement, index, remove, obj) {
		var css = remove ? "" : obj.css;

		if (styleElement.styleSheet) {
			styleElement.styleSheet.cssText = replaceText(index, css);
		} else {
			var cssNode = document.createTextNode(css);
			var childNodes = styleElement.childNodes;
			if (childNodes[index]) styleElement.removeChild(childNodes[index]);
			if (childNodes.length) {
				styleElement.insertBefore(cssNode, childNodes[index]);
			} else {
				styleElement.appendChild(cssNode);
			}
		}
	}

	function applyToTag(styleElement, obj) {
		var css = obj.css;
		var media = obj.media;

		if(media) {
			styleElement.setAttribute("media", media)
		}

		if(styleElement.styleSheet) {
			styleElement.styleSheet.cssText = css;
		} else {
			while(styleElement.firstChild) {
				styleElement.removeChild(styleElement.firstChild);
			}
			styleElement.appendChild(document.createTextNode(css));
		}
	}

	function updateLink(linkElement, obj) {
		var css = obj.css;
		var sourceMap = obj.sourceMap;

		if(sourceMap) {
			// http://stackoverflow.com/a/26603875
			css += "\n/*# sourceMappingURL=data:application/json;base64," + btoa(unescape(encodeURIComponent(JSON.stringify(sourceMap)))) + " */";
		}

		var blob = new Blob([css], { type: "text/css" });

		var oldSrc = linkElement.href;

		linkElement.href = URL.createObjectURL(blob);

		if(oldSrc)
			URL.revokeObjectURL(oldSrc);
	}


/***/ },
/* 11 */
/***/ function(module, exports, __webpack_require__) {

	// style-loader: Adds some css to the DOM by adding a <style> tag

	// load the styles
	var content = __webpack_require__(12);
	if(typeof content === 'string') content = [[module.id, content, '']];
	// add the styles to the DOM
	var update = __webpack_require__(10)(content, {});
	if(content.locals) module.exports = content.locals;
	// Hot Module Replacement
	if(false) {
		// When the styles change, update the <style> tags
		if(!content.locals) {
			module.hot.accept("!!./../../node_modules/css-loader/index.js!./gakHeader.css", function() {
				var newContent = require("!!./../../node_modules/css-loader/index.js!./gakHeader.css");
				if(typeof newContent === 'string') newContent = [[module.id, newContent, '']];
				update(newContent);
			});
		}
		// When the module is disposed, remove the <style> tags
		module.hot.dispose(function() { update(); });
	}

/***/ },
/* 12 */
/***/ function(module, exports, __webpack_require__) {

	exports = module.exports = __webpack_require__(4)();
	// imports


	// module
	exports.push([module.id, "body {\n\tbackground-color: #e1e1e1;\n}\n\nheader {\n\tbackground-color: #e1e1e1;\n}\n\n.center-container {\n\tbackground-color: white;\n\tpadding-left: 20px;\n\tpadding-right: 20px;\n}\n\n.top-header {\n    width: 100%;\n    /*position: fixed;\n    z-index: 99999;\n    */\n}\n\n.header-text {\n\t\tmargin-top: 60px;\n\t\tmargin-bottom: -20px;\n\t\ttext-transform: uppercase;\n\t\tfont-size: 62px;\n    font-weight: bold;\n    text-align: center;\n}\n\n.logo {\n\t\twidth: 65%;\n\t\theight: auto; \n    margin-bottom: 25px;\n    margin-top: 15px;\n    transition: all .5s ease 0s;\n    -webkit-transition: all .5s ease 0s;\n    -moz-transition: all .5s ease 0s;\n    -o-transition: all .5s ease 0s;\n    -ms-transition: all .5s ease 0s;\n}\n\nli a.active {\n    color: #42b3e5 !important;\n}\n\n.navbar-default {\n    background-color: transparent;\n    border-color: transparent;\n    margin-bottom: 0;\n    margin-top: 4px;\n    transition: all .5s ease 0s;\n    -webkit-transition: all .5s ease 0s;\n    -moz-transition: all .5s ease 0s;\n    -o-transition: all .5s ease 0s;\n    -ms-transition: all .5s ease 0s;\n}\n\n.navbar-default .navbar-nav {\n\t\ttext-align: center;\n\t\tpadding: 0px;\n\t\toverflow: hidden;\n\t\tzoom: 1;\n}\n\n.navbar-default .navbar-nav>li {\n\t\tpadding: 26px;\n\t\tdisplay: inline-block;\n}\n\n.navbar-default .navbar-nav>li>a {\n    font-size: 20px;\n    color: #737373;\n    text-transform: uppercase;\n    transition: all .7s ease 0s;\n    -webkit-transition: all .7s ease 0s;\n    -moz-transition: all .7s ease 0s;\n    -o-transition: all .7s ease 0s;\n    -ms-transition: all .7s ease 0s;\n    padding: 14px 15px;\n    font-weight: bold;\n}\n\n.navbar-default .navbar-nav>li>a:hover {\n    color: #42b3e5;\n}\n\n.hrline {\n\t\tborder-bottom: thick solid #000;\n\t\tmargin-bottom: 20px;\n}\n", ""]);

	// exports


/***/ },
/* 13 */
/***/ function(module, exports, __webpack_require__) {

	// style-loader: Adds some css to the DOM by adding a <style> tag

	// load the styles
	var content = __webpack_require__(14);
	if(typeof content === 'string') content = [[module.id, content, '']];
	// add the styles to the DOM
	var update = __webpack_require__(10)(content, {});
	if(content.locals) module.exports = content.locals;
	// Hot Module Replacement
	if(false) {
		// When the styles change, update the <style> tags
		if(!content.locals) {
			module.hot.accept("!!./../../node_modules/css-loader/index.js!./gakFooter.css", function() {
				var newContent = require("!!./../../node_modules/css-loader/index.js!./gakFooter.css");
				if(typeof newContent === 'string') newContent = [[module.id, newContent, '']];
				update(newContent);
			});
		}
		// When the module is disposed, remove the <style> tags
		module.hot.dispose(function() { update(); });
	}

/***/ },
/* 14 */
/***/ function(module, exports, __webpack_require__) {

	exports = module.exports = __webpack_require__(4)();
	// imports


	// module
	exports.push([module.id, "html {\n\t\tposition: relative;\n\t\tmin-height: 100%;\n}\n\nfooter{\n\tbackground-color: #e1e1e1;\n}\n\nbody {\n\t\tmargin-bottom: 60px;\n}\n\n.footer {\n\t\tposition: absolute;\n\t\tbottom: 0;\n\t\twidth: 100%;\n\t\theight: 60px;\n}\n\n.hrLineTop {\n\t\tborder-top: thick solid #000;\n\t\tmargin-top: 20px;\n}\n", ""]);

	// exports


/***/ },
/* 15 */
/***/ function(module, exports, __webpack_require__) {

	var __WEBPACK_AMD_DEFINE_FACTORY__, __WEBPACK_AMD_DEFINE_ARRAY__, __WEBPACK_AMD_DEFINE_RESULT__;/*!
	 * mustache.js - Logic-less {{mustache}} templates with JavaScript
	 * http://github.com/janl/mustache.js
	 */

	/*global define: false Mustache: true*/

	(function defineMustache (global, factory) {
	  if (typeof exports === 'object' && exports && typeof exports.nodeName !== 'string') {
	    factory(exports); // CommonJS
	  } else if (true) {
	    !(__WEBPACK_AMD_DEFINE_ARRAY__ = [exports], __WEBPACK_AMD_DEFINE_FACTORY__ = (factory), __WEBPACK_AMD_DEFINE_RESULT__ = (typeof __WEBPACK_AMD_DEFINE_FACTORY__ === 'function' ? (__WEBPACK_AMD_DEFINE_FACTORY__.apply(exports, __WEBPACK_AMD_DEFINE_ARRAY__)) : __WEBPACK_AMD_DEFINE_FACTORY__), __WEBPACK_AMD_DEFINE_RESULT__ !== undefined && (module.exports = __WEBPACK_AMD_DEFINE_RESULT__)); // AMD
	  } else {
	    global.Mustache = {};
	    factory(global.Mustache); // script, wsh, asp
	  }
	}(this, function mustacheFactory (mustache) {

	  var objectToString = Object.prototype.toString;
	  var isArray = Array.isArray || function isArrayPolyfill (object) {
	    return objectToString.call(object) === '[object Array]';
	  };

	  function isFunction (object) {
	    return typeof object === 'function';
	  }

	  /**
	   * More correct typeof string handling array
	   * which normally returns typeof 'object'
	   */
	  function typeStr (obj) {
	    return isArray(obj) ? 'array' : typeof obj;
	  }

	  function escapeRegExp (string) {
	    return string.replace(/[\-\[\]{}()*+?.,\\\^$|#\s]/g, '\\$&');
	  }

	  /**
	   * Null safe way of checking whether or not an object,
	   * including its prototype, has a given property
	   */
	  function hasProperty (obj, propName) {
	    return obj != null && typeof obj === 'object' && (propName in obj);
	  }

	  // Workaround for https://issues.apache.org/jira/browse/COUCHDB-577
	  // See https://github.com/janl/mustache.js/issues/189
	  var regExpTest = RegExp.prototype.test;
	  function testRegExp (re, string) {
	    return regExpTest.call(re, string);
	  }

	  var nonSpaceRe = /\S/;
	  function isWhitespace (string) {
	    return !testRegExp(nonSpaceRe, string);
	  }

	  var entityMap = {
	    '&': '&amp;',
	    '<': '&lt;',
	    '>': '&gt;',
	    '"': '&quot;',
	    "'": '&#39;',
	    '/': '&#x2F;',
	    '`': '&#x60;',
	    '=': '&#x3D;'
	  };

	  function escapeHtml (string) {
	    return String(string).replace(/[&<>"'`=\/]/g, function fromEntityMap (s) {
	      return entityMap[s];
	    });
	  }

	  var whiteRe = /\s*/;
	  var spaceRe = /\s+/;
	  var equalsRe = /\s*=/;
	  var curlyRe = /\s*\}/;
	  var tagRe = /#|\^|\/|>|\{|&|=|!/;

	  /**
	   * Breaks up the given `template` string into a tree of tokens. If the `tags`
	   * argument is given here it must be an array with two string values: the
	   * opening and closing tags used in the template (e.g. [ "<%", "%>" ]). Of
	   * course, the default is to use mustaches (i.e. mustache.tags).
	   *
	   * A token is an array with at least 4 elements. The first element is the
	   * mustache symbol that was used inside the tag, e.g. "#" or "&". If the tag
	   * did not contain a symbol (i.e. {{myValue}}) this element is "name". For
	   * all text that appears outside a symbol this element is "text".
	   *
	   * The second element of a token is its "value". For mustache tags this is
	   * whatever else was inside the tag besides the opening symbol. For text tokens
	   * this is the text itself.
	   *
	   * The third and fourth elements of the token are the start and end indices,
	   * respectively, of the token in the original template.
	   *
	   * Tokens that are the root node of a subtree contain two more elements: 1) an
	   * array of tokens in the subtree and 2) the index in the original template at
	   * which the closing tag for that section begins.
	   */
	  function parseTemplate (template, tags) {
	    if (!template)
	      return [];

	    var sections = [];     // Stack to hold section tokens
	    var tokens = [];       // Buffer to hold the tokens
	    var spaces = [];       // Indices of whitespace tokens on the current line
	    var hasTag = false;    // Is there a {{tag}} on the current line?
	    var nonSpace = false;  // Is there a non-space char on the current line?

	    // Strips all whitespace tokens array for the current line
	    // if there was a {{#tag}} on it and otherwise only space.
	    function stripSpace () {
	      if (hasTag && !nonSpace) {
	        while (spaces.length)
	          delete tokens[spaces.pop()];
	      } else {
	        spaces = [];
	      }

	      hasTag = false;
	      nonSpace = false;
	    }

	    var openingTagRe, closingTagRe, closingCurlyRe;
	    function compileTags (tagsToCompile) {
	      if (typeof tagsToCompile === 'string')
	        tagsToCompile = tagsToCompile.split(spaceRe, 2);

	      if (!isArray(tagsToCompile) || tagsToCompile.length !== 2)
	        throw new Error('Invalid tags: ' + tagsToCompile);

	      openingTagRe = new RegExp(escapeRegExp(tagsToCompile[0]) + '\\s*');
	      closingTagRe = new RegExp('\\s*' + escapeRegExp(tagsToCompile[1]));
	      closingCurlyRe = new RegExp('\\s*' + escapeRegExp('}' + tagsToCompile[1]));
	    }

	    compileTags(tags || mustache.tags);

	    var scanner = new Scanner(template);

	    var start, type, value, chr, token, openSection;
	    while (!scanner.eos()) {
	      start = scanner.pos;

	      // Match any text between tags.
	      value = scanner.scanUntil(openingTagRe);

	      if (value) {
	        for (var i = 0, valueLength = value.length; i < valueLength; ++i) {
	          chr = value.charAt(i);

	          if (isWhitespace(chr)) {
	            spaces.push(tokens.length);
	          } else {
	            nonSpace = true;
	          }

	          tokens.push([ 'text', chr, start, start + 1 ]);
	          start += 1;

	          // Check for whitespace on the current line.
	          if (chr === '\n')
	            stripSpace();
	        }
	      }

	      // Match the opening tag.
	      if (!scanner.scan(openingTagRe))
	        break;

	      hasTag = true;

	      // Get the tag type.
	      type = scanner.scan(tagRe) || 'name';
	      scanner.scan(whiteRe);

	      // Get the tag value.
	      if (type === '=') {
	        value = scanner.scanUntil(equalsRe);
	        scanner.scan(equalsRe);
	        scanner.scanUntil(closingTagRe);
	      } else if (type === '{') {
	        value = scanner.scanUntil(closingCurlyRe);
	        scanner.scan(curlyRe);
	        scanner.scanUntil(closingTagRe);
	        type = '&';
	      } else {
	        value = scanner.scanUntil(closingTagRe);
	      }

	      // Match the closing tag.
	      if (!scanner.scan(closingTagRe))
	        throw new Error('Unclosed tag at ' + scanner.pos);

	      token = [ type, value, start, scanner.pos ];
	      tokens.push(token);

	      if (type === '#' || type === '^') {
	        sections.push(token);
	      } else if (type === '/') {
	        // Check section nesting.
	        openSection = sections.pop();

	        if (!openSection)
	          throw new Error('Unopened section "' + value + '" at ' + start);

	        if (openSection[1] !== value)
	          throw new Error('Unclosed section "' + openSection[1] + '" at ' + start);
	      } else if (type === 'name' || type === '{' || type === '&') {
	        nonSpace = true;
	      } else if (type === '=') {
	        // Set the tags for the next time around.
	        compileTags(value);
	      }
	    }

	    // Make sure there are no open sections when we're done.
	    openSection = sections.pop();

	    if (openSection)
	      throw new Error('Unclosed section "' + openSection[1] + '" at ' + scanner.pos);

	    return nestTokens(squashTokens(tokens));
	  }

	  /**
	   * Combines the values of consecutive text tokens in the given `tokens` array
	   * to a single token.
	   */
	  function squashTokens (tokens) {
	    var squashedTokens = [];

	    var token, lastToken;
	    for (var i = 0, numTokens = tokens.length; i < numTokens; ++i) {
	      token = tokens[i];

	      if (token) {
	        if (token[0] === 'text' && lastToken && lastToken[0] === 'text') {
	          lastToken[1] += token[1];
	          lastToken[3] = token[3];
	        } else {
	          squashedTokens.push(token);
	          lastToken = token;
	        }
	      }
	    }

	    return squashedTokens;
	  }

	  /**
	   * Forms the given array of `tokens` into a nested tree structure where
	   * tokens that represent a section have two additional items: 1) an array of
	   * all tokens that appear in that section and 2) the index in the original
	   * template that represents the end of that section.
	   */
	  function nestTokens (tokens) {
	    var nestedTokens = [];
	    var collector = nestedTokens;
	    var sections = [];

	    var token, section;
	    for (var i = 0, numTokens = tokens.length; i < numTokens; ++i) {
	      token = tokens[i];

	      switch (token[0]) {
	        case '#':
	        case '^':
	          collector.push(token);
	          sections.push(token);
	          collector = token[4] = [];
	          break;
	        case '/':
	          section = sections.pop();
	          section[5] = token[2];
	          collector = sections.length > 0 ? sections[sections.length - 1][4] : nestedTokens;
	          break;
	        default:
	          collector.push(token);
	      }
	    }

	    return nestedTokens;
	  }

	  /**
	   * A simple string scanner that is used by the template parser to find
	   * tokens in template strings.
	   */
	  function Scanner (string) {
	    this.string = string;
	    this.tail = string;
	    this.pos = 0;
	  }

	  /**
	   * Returns `true` if the tail is empty (end of string).
	   */
	  Scanner.prototype.eos = function eos () {
	    return this.tail === '';
	  };

	  /**
	   * Tries to match the given regular expression at the current position.
	   * Returns the matched text if it can match, the empty string otherwise.
	   */
	  Scanner.prototype.scan = function scan (re) {
	    var match = this.tail.match(re);

	    if (!match || match.index !== 0)
	      return '';

	    var string = match[0];

	    this.tail = this.tail.substring(string.length);
	    this.pos += string.length;

	    return string;
	  };

	  /**
	   * Skips all text until the given regular expression can be matched. Returns
	   * the skipped string, which is the entire tail if no match can be made.
	   */
	  Scanner.prototype.scanUntil = function scanUntil (re) {
	    var index = this.tail.search(re), match;

	    switch (index) {
	      case -1:
	        match = this.tail;
	        this.tail = '';
	        break;
	      case 0:
	        match = '';
	        break;
	      default:
	        match = this.tail.substring(0, index);
	        this.tail = this.tail.substring(index);
	    }

	    this.pos += match.length;

	    return match;
	  };

	  /**
	   * Represents a rendering context by wrapping a view object and
	   * maintaining a reference to the parent context.
	   */
	  function Context (view, parentContext) {
	    this.view = view;
	    this.cache = { '.': this.view };
	    this.parent = parentContext;
	  }

	  /**
	   * Creates a new context using the given view with this context
	   * as the parent.
	   */
	  Context.prototype.push = function push (view) {
	    return new Context(view, this);
	  };

	  /**
	   * Returns the value of the given name in this context, traversing
	   * up the context hierarchy if the value is absent in this context's view.
	   */
	  Context.prototype.lookup = function lookup (name) {
	    var cache = this.cache;

	    var value;
	    if (cache.hasOwnProperty(name)) {
	      value = cache[name];
	    } else {
	      var context = this, names, index, lookupHit = false;

	      while (context) {
	        if (name.indexOf('.') > 0) {
	          value = context.view;
	          names = name.split('.');
	          index = 0;

	          /**
	           * Using the dot notion path in `name`, we descend through the
	           * nested objects.
	           *
	           * To be certain that the lookup has been successful, we have to
	           * check if the last object in the path actually has the property
	           * we are looking for. We store the result in `lookupHit`.
	           *
	           * This is specially necessary for when the value has been set to
	           * `undefined` and we want to avoid looking up parent contexts.
	           **/
	          while (value != null && index < names.length) {
	            if (index === names.length - 1)
	              lookupHit = hasProperty(value, names[index]);

	            value = value[names[index++]];
	          }
	        } else {
	          value = context.view[name];
	          lookupHit = hasProperty(context.view, name);
	        }

	        if (lookupHit)
	          break;

	        context = context.parent;
	      }

	      cache[name] = value;
	    }

	    if (isFunction(value))
	      value = value.call(this.view);

	    return value;
	  };

	  /**
	   * A Writer knows how to take a stream of tokens and render them to a
	   * string, given a context. It also maintains a cache of templates to
	   * avoid the need to parse the same template twice.
	   */
	  function Writer () {
	    this.cache = {};
	  }

	  /**
	   * Clears all cached templates in this writer.
	   */
	  Writer.prototype.clearCache = function clearCache () {
	    this.cache = {};
	  };

	  /**
	   * Parses and caches the given `template` and returns the array of tokens
	   * that is generated from the parse.
	   */
	  Writer.prototype.parse = function parse (template, tags) {
	    var cache = this.cache;
	    var tokens = cache[template];

	    if (tokens == null)
	      tokens = cache[template] = parseTemplate(template, tags);

	    return tokens;
	  };

	  /**
	   * High-level method that is used to render the given `template` with
	   * the given `view`.
	   *
	   * The optional `partials` argument may be an object that contains the
	   * names and templates of partials that are used in the template. It may
	   * also be a function that is used to load partial templates on the fly
	   * that takes a single argument: the name of the partial.
	   */
	  Writer.prototype.render = function render (template, view, partials) {
	    var tokens = this.parse(template);
	    var context = (view instanceof Context) ? view : new Context(view);
	    return this.renderTokens(tokens, context, partials, template);
	  };

	  /**
	   * Low-level method that renders the given array of `tokens` using
	   * the given `context` and `partials`.
	   *
	   * Note: The `originalTemplate` is only ever used to extract the portion
	   * of the original template that was contained in a higher-order section.
	   * If the template doesn't use higher-order sections, this argument may
	   * be omitted.
	   */
	  Writer.prototype.renderTokens = function renderTokens (tokens, context, partials, originalTemplate) {
	    var buffer = '';

	    var token, symbol, value;
	    for (var i = 0, numTokens = tokens.length; i < numTokens; ++i) {
	      value = undefined;
	      token = tokens[i];
	      symbol = token[0];

	      if (symbol === '#') value = this.renderSection(token, context, partials, originalTemplate);
	      else if (symbol === '^') value = this.renderInverted(token, context, partials, originalTemplate);
	      else if (symbol === '>') value = this.renderPartial(token, context, partials, originalTemplate);
	      else if (symbol === '&') value = this.unescapedValue(token, context);
	      else if (symbol === 'name') value = this.escapedValue(token, context);
	      else if (symbol === 'text') value = this.rawValue(token);

	      if (value !== undefined)
	        buffer += value;
	    }

	    return buffer;
	  };

	  Writer.prototype.renderSection = function renderSection (token, context, partials, originalTemplate) {
	    var self = this;
	    var buffer = '';
	    var value = context.lookup(token[1]);

	    // This function is used to render an arbitrary template
	    // in the current context by higher-order sections.
	    function subRender (template) {
	      return self.render(template, context, partials);
	    }

	    if (!value) return;

	    if (isArray(value)) {
	      for (var j = 0, valueLength = value.length; j < valueLength; ++j) {
	        buffer += this.renderTokens(token[4], context.push(value[j]), partials, originalTemplate);
	      }
	    } else if (typeof value === 'object' || typeof value === 'string' || typeof value === 'number') {
	      buffer += this.renderTokens(token[4], context.push(value), partials, originalTemplate);
	    } else if (isFunction(value)) {
	      if (typeof originalTemplate !== 'string')
	        throw new Error('Cannot use higher-order sections without the original template');

	      // Extract the portion of the original template that the section contains.
	      value = value.call(context.view, originalTemplate.slice(token[3], token[5]), subRender);

	      if (value != null)
	        buffer += value;
	    } else {
	      buffer += this.renderTokens(token[4], context, partials, originalTemplate);
	    }
	    return buffer;
	  };

	  Writer.prototype.renderInverted = function renderInverted (token, context, partials, originalTemplate) {
	    var value = context.lookup(token[1]);

	    // Use JavaScript's definition of falsy. Include empty arrays.
	    // See https://github.com/janl/mustache.js/issues/186
	    if (!value || (isArray(value) && value.length === 0))
	      return this.renderTokens(token[4], context, partials, originalTemplate);
	  };

	  Writer.prototype.renderPartial = function renderPartial (token, context, partials) {
	    if (!partials) return;

	    var value = isFunction(partials) ? partials(token[1]) : partials[token[1]];
	    if (value != null)
	      return this.renderTokens(this.parse(value), context, partials, value);
	  };

	  Writer.prototype.unescapedValue = function unescapedValue (token, context) {
	    var value = context.lookup(token[1]);
	    if (value != null)
	      return value;
	  };

	  Writer.prototype.escapedValue = function escapedValue (token, context) {
	    var value = context.lookup(token[1]);
	    if (value != null)
	      return mustache.escape(value);
	  };

	  Writer.prototype.rawValue = function rawValue (token) {
	    return token[1];
	  };

	  mustache.name = 'mustache.js';
	  mustache.version = '2.2.1';
	  mustache.tags = [ '{{', '}}' ];

	  // All high-level mustache.* functions use this writer.
	  var defaultWriter = new Writer();

	  /**
	   * Clears all cached templates in the default writer.
	   */
	  mustache.clearCache = function clearCache () {
	    return defaultWriter.clearCache();
	  };

	  /**
	   * Parses and caches the given template in the default writer and returns the
	   * array of tokens it contains. Doing this ahead of time avoids the need to
	   * parse templates on the fly as they are rendered.
	   */
	  mustache.parse = function parse (template, tags) {
	    return defaultWriter.parse(template, tags);
	  };

	  /**
	   * Renders the `template` with the given `view` and `partials` using the
	   * default writer.
	   */
	  mustache.render = function render (template, view, partials) {
	    if (typeof template !== 'string') {
	      throw new TypeError('Invalid template! Template should be a "string" ' +
	                          'but "' + typeStr(template) + '" was given as the first ' +
	                          'argument for mustache#render(template, view, partials)');
	    }

	    return defaultWriter.render(template, view, partials);
	  };

	  // This is here for backwards compatibility with 0.4.x.,
	  /*eslint-disable */ // eslint wants camel cased function name
	  mustache.to_html = function to_html (template, view, partials, send) {
	    /*eslint-enable*/

	    var result = mustache.render(template, view, partials);

	    if (isFunction(send)) {
	      send(result);
	    } else {
	      return result;
	    }
	  };

	  // Export the escaping function so that the user may override it.
	  // See https://github.com/janl/mustache.js/issues/244
	  mustache.escape = escapeHtml;

	  // Export these mainly for testing, but also for advanced usage.
	  mustache.Scanner = Scanner;
	  mustache.Context = Context;
	  mustache.Writer = Writer;

	}));


/***/ },
/* 16 */
/***/ function(module, exports, __webpack_require__) {

	'use strict';

	Object.defineProperty(exports, "__esModule", {
		value: true
	});

	var _jquery = __webpack_require__(1);

	var _jquery2 = _interopRequireDefault(_jquery);

	var _mustache = __webpack_require__(15);

	var _mustache2 = _interopRequireDefault(_mustache);

	var _videos = __webpack_require__(17);

	var _videos2 = _interopRequireDefault(_videos);

	var _video = __webpack_require__(18);

	var _video2 = _interopRequireDefault(_video);

	var _singleVideo = __webpack_require__(20);

	var _singleVideo2 = _interopRequireDefault(_singleVideo);

	function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

	function addClickListerns() {
		(0, _jquery2.default)('.vidThumb').on('click', function (event) {
			event.preventDefault();
			//remove old
			(0, _jquery2.default)('.selected-vid').addClass('col-md-4');
			(0, _jquery2.default)('.selected-vid').removeClass('col-md-12');
			(0, _jquery2.default)('.selected-vid').children().show();
			(0, _jquery2.default)('.selected-vid > iframe').remove();

			//show new video
			var html = '<iframe src="https://www.facebook.com/plugins/video.php?href=https%3A%2F%2Fwww.facebook.com%2F338312802949307%2Fvideos%2Fvb.338312802949307%2F' + event.target.id + '%2F%3Ftype%3D2%26theater&width=1150&show_text=false&height=545&appId" width="1150" height="545" style="border:none;overflow:hidden" scrolling="no" frameborder="0" allowTransparency="true"></iframe>';
			html = _jquery2.default.parseHTML(html);
			(0, _jquery2.default)('#' + event.target.id).parent().children().hide();
			(0, _jquery2.default)('#' + event.target.id).parent().removeClass('col-md-4');
			(0, _jquery2.default)('#' + event.target.id).parent().addClass('col-md-12');
			(0, _jquery2.default)('#' + event.target.id).parent().addClass('selected-vid');
			(0, _jquery2.default)('#' + event.target.id).parent().append(html);
		});
	}

	exports.default = {
		loadVids: function loadVids() {
			var counter = 0;
			_videos2.default.forEach(function (vidobj) {
				(0, _jquery2.default)('#vidrow1').append(_mustache2.default.render(_singleVideo2.default, vidobj));
				counter++;
				if (counter === _videos2.default.length) {
					addClickListerns();
				}
			});
		}
	};

/***/ },
/* 17 */
/***/ function(module, exports) {

	module.exports = [
		{
			"name": "Klubbstevne 30.04.2016",
			"faceID": "969600336487214"
		},
		{
			"name": "RM 2015",
			"faceID": "855416314572284"
		},
		{
			"name": "NM 5kamp 2015",
			"faceID": "843994999047749"
		},
		{
			"name": "Klubbstevne 18.04.15",
			"faceID": "773343922779524"
		},
		{
			"name": "ØM 2015",
			"faceID": "737032736410643"
		}
	];

/***/ },
/* 18 */
/***/ function(module, exports, __webpack_require__) {

	// style-loader: Adds some css to the DOM by adding a <style> tag

	// load the styles
	var content = __webpack_require__(19);
	if(typeof content === 'string') content = [[module.id, content, '']];
	// add the styles to the DOM
	var update = __webpack_require__(10)(content, {});
	if(content.locals) module.exports = content.locals;
	// Hot Module Replacement
	if(false) {
		// When the styles change, update the <style> tags
		if(!content.locals) {
			module.hot.accept("!!./../../node_modules/css-loader/index.js!./video.css", function() {
				var newContent = require("!!./../../node_modules/css-loader/index.js!./video.css");
				if(typeof newContent === 'string') newContent = [[module.id, newContent, '']];
				update(newContent);
			});
		}
		// When the module is disposed, remove the <style> tags
		module.hot.dispose(function() { update(); });
	}

/***/ },
/* 19 */
/***/ function(module, exports, __webpack_require__) {

	exports = module.exports = __webpack_require__(4)();
	// imports


	// module
	exports.push([module.id, ".video-col {\n\t\tmargin-bottom: 30px;\n}\n\n.vidThumb {\n\t\twidth: 100%;\n\t\theight: auto;\n}\n\n.caption > h3 {\n\t\ttext-align: center;\n}\n", ""]);

	// exports


/***/ },
/* 20 */
/***/ function(module, exports) {

	module.exports = "<div class='video-col col-md-4'>\n\t\t<img class=\"vidThumb\" id=\"{{faceID}}\" src=\"https://graph.facebook.com/{{faceID}}/picture\">\n\t\t<div class='caption'>\n\t\t\t<h3> {{name}} </h3>\n\t\t</div>\n</div>\n";

/***/ },
/* 21 */
/***/ function(module, exports, __webpack_require__) {

	'use strict';

	Object.defineProperty(exports, "__esModule", {
		value: true
	});

	var _jquery = __webpack_require__(1);

	var _jquery2 = _interopRequireDefault(_jquery);

	var _mustache = __webpack_require__(15);

	var _mustache2 = _interopRequireDefault(_mustache);

	var _news = __webpack_require__(22);

	var _news2 = _interopRequireDefault(_news);

	var _home = __webpack_require__(23);

	var _home2 = _interopRequireDefault(_home);

	var _story = __webpack_require__(25);

	var _story2 = _interopRequireDefault(_story);

	function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

	exports.default = {
		loadNews: function loadVids() {
			_news2.default.forEach(function (story) {
				(0, _jquery2.default)('#storyArea').append(_mustache2.default.render(_story2.default, story));
			});
		}
	};

/***/ },
/* 22 */
/***/ function(module, exports) {

	module.exports = [
		{
			"author": "Tor Kristoffer Kletthagen",
			"date": "31.01.2016",
			"header": "Regionsamling vinter",
			"text": "Østlandet Vektløfterregion har en fast sommersamling i året og den er det Gjøvik AK som arrangerer. På Gjøvik AK´s initiativ er det nå blitt en fast vintersamling også. Regionens første vintersamling hadde 13 deltagere, hvorav 8 kom fra andre klubber. Gledelig var det at 2 stykker fra Vigrestad IK la NM-oppkjøringen sin hos oss denne helgen. Samlingen ble ledet av Olav Johansen og Tor Eric Sivertsen. Treningen hadde hovedfokus på fart, korrekt teknikk og at atletene skulle bli mer klar over egen teknikk. Samlingen bestod av 3 økter med vektløfting, 1 økt med power-yoga og et kostholdsforedrag av Aktivitetsspesialtisten Janne Muldal. Det er gledelig å se treningsiveren blant klubbens medlemmer. Forhåpentligvis vil fremtidige treningssamlinger samle enda flere atleter og øke nivået i regionen.",
			"img": "./src/img/vintersamling.jpg"
		},
		{
			"author": "Tor Eric Sivertsen",
			"date": "27.01.2016",
			"header": "ØM 2015",
			"text": "I helgen var Gjøvik atletklubb ute på tur igjen, denne gangen var det Østlandsmesterskap som fant sted i Spydeberghallen. Denne gangen var det 5 representanter som løftet for Gjøvik, Kristian Holm, Åsmund Rykhus, Jardar Tøn, Dag Alexander Klethagen og Tor Eric Sivertsen. Først ut fra Gjøvik var Tor Eric Sivertsen (69Kg klassen) som var på komfortable vekter denne gangen, og gjorde 62,65,68 i rykk og 80,83,86 i støt. Andre mann ut var Åsmund Rykhus (85Kg klassen) som med dette stevnet debuterte på stevne utenfor klubben og fikk rykkserien 67,70,74 to kg pers og en knall debut! :) I støt bomma han desverre i overstøt på 1løftet på 97Kg men tok seg sammen og satte det på plass i andre! i tredje forsøk fikk Åsmund pers i vending på 102Kg men overstøtet ble for mye denne gangen. Dette ble en fin sammenlagt på 171Kg. Jardar Tøn (85Kg klassen) åpnet friskt i rykk med en pers med en kg, og satte dermed på plass 92Kg! Det ble høynet til 96Kg i andre rykk som han drog seg under to ganger, men desverre ikke greide å komme opp med denne gangen. I støt startet han også friskt, bare tre kg under pers men satte dette på plass i 2 forsøk. Dette holdt for Jardar denne gangen. Ett flott resultat på 199Kg sammenlagt, etter en lang periode med bare teknisk og lett løfting! Her kommer det snart mer kg på stangen. Kristian Holm (69Kg) klassen, er en ivrig ung atlet og gikk på med frisk mot! Den vel ivrige unggutten bomma det første rykket sitt på 33Kg, men tok seg sammen og viste at dette er lette vekter i andre forsøk! i tredje følger han opp med tangering av pers på 36Kg. I støt fikk han 42Kg i første forsøk, i andre ble det høynet til 45Kg som han rotet bort teknisk, men unge Kristian er bestemt og satte det på plass i 3 forsøk! Dette blir ett flott resultat på 81Kg og mer i vente. Rutinerte Dag Alexander Klethagen (94Kg klassen) gikk ut på meget komfortable vekter i rykk og fikk med det serien: 85,92 og 98kg i god 'Daggern stil' ble løftene bare finere og finere. I støt Skulle Dag Alexander vise seg å være i form til tross for en dårlig treningsperiode i forkant. Der satte han serien 125,132 og 137Kg Dette tok han i styrkevending og dermed ble det pers for han i styrkevending! Desverre ble det bom i siste overstøt og Dag Alexander ble stående med 98Kg i rykk og 132 i støt. 230Kg sammenlagt. Takk til coach Olav Johansen som tok seg av atletene på stevnet.",
			"img": "./src/img/rm2016.jpg"
		}
	];

/***/ },
/* 23 */
/***/ function(module, exports, __webpack_require__) {

	// style-loader: Adds some css to the DOM by adding a <style> tag

	// load the styles
	var content = __webpack_require__(24);
	if(typeof content === 'string') content = [[module.id, content, '']];
	// add the styles to the DOM
	var update = __webpack_require__(10)(content, {});
	if(content.locals) module.exports = content.locals;
	// Hot Module Replacement
	if(false) {
		// When the styles change, update the <style> tags
		if(!content.locals) {
			module.hot.accept("!!./../../node_modules/css-loader/index.js!./home.css", function() {
				var newContent = require("!!./../../node_modules/css-loader/index.js!./home.css");
				if(typeof newContent === 'string') newContent = [[module.id, newContent, '']];
				update(newContent);
			});
		}
		// When the module is disposed, remove the <style> tags
		module.hot.dispose(function() { update(); });
	}

/***/ },
/* 24 */
/***/ function(module, exports, __webpack_require__) {

	exports = module.exports = __webpack_require__(4)();
	// imports


	// module
	exports.push([module.id, ".story {\n\tmargin-bottom: 90px;\n}\n\n.storyArea {\n}\n", ""]);

	// exports


/***/ },
/* 25 */
/***/ function(module, exports) {

	module.exports = "<div class='story'>\n\t<h3> {{ header }} </h3>\n\t<p>{{ date }} av {{ author }} </p>\n\t<hr></hr>\n\t<img class='img-responsive' src=\"{{img}}\"></img>\n\t<hr></hr>\n\t<p> {{text}}</p>\n\t<hr></hr>\n</div>\n";

/***/ },
/* 26 */
/***/ function(module, exports, __webpack_require__) {

	module.exports = "<div class='page-header'>\n\t<h1> Nyheter <small></small> </h1>\n</div>\n\n<div class='row'>\n\n\t<div id=\"storyArea\" class='col-lg-8'>\n\n\t</div>\n\t<!-- sidebar  -->\n\t<div class='col-lg-4'>\n\n\t\t<div class='panel panel-default'>\n\t\t\t<div class='panel-heading'>\n\t\t\t\t<h3 class='panel-title'> <strong> Besøk oss på facebook </strong> </h3>\n\t\t\t</div>\n\t\t\t<div class='panel-body'>\n\t\t\t<iframe src=\"https://www.facebook.com/plugins/page.php?href=https%3A%2F%2Fwww.facebook.com%2FGj%C3%B8vik-Atletklubb-338312802949307%2F&tabs&width=340&height=214&small_header=false&adapt_container_width=true&hide_cover=false&show_facepile=true&appId\" width=\"340\" height=\"214\" style=\"border:none;overflow:hidden\" scrolling=\"no\" frameborder=\"0\" allowTransparency=\"true\"></iframe>\t\n\t\t\t</div>\n\t\t</div>\n\n\t\t<div class='panel panel-default'>\n\t\t\t<div class='panel-heading'>\n\t\t\t\t<h3 class='panel-title'> <strong> Sponsorer og sammarbeidspartnere </strong> </h3>\n\t\t\t</div>\n\t\t\t<div class='panel-body'>\n\t\t\t\t<img class='img-responsive' src=\"" + __webpack_require__(27) + "\"> </img>\n\t\t\t\t<br></br>\n\t\t\t\t<img class='img-responsive' src=\"" + __webpack_require__(28) + "\"> </img>\n\t\t\t\t<br></br>\n\t\t\t\t<img class='img-responsive' src=\"" + __webpack_require__(29) + "\"> </img>\n\t\t\t</div>\n\t\t</div>\n\n\t\t<div class='panel panel-default'>\n\t\t\t<div class='panel-heading'>\n\t\t\t\t<h3 class='panel-title'> <strong> Nyttige Lenker </strong> </h3>\n\t\t\t</div>\n\t\t\t<div class='panel-body'>\n\t\t\t\t<p><a> Norges Vektlofterforbund </a></p>\n\t\t\t\t<p><a> Lover og Regler Norges Vektlofterforbund </a></p>\n\t\t\t\t<p><a> Olympiatoppen </a></p>\n\t\t\t\t<p><a> Norges idreddsforbund </a></p>\n\t\t\t\t<p><a> Antidoping norge </a></p>\n\t\t\t</div>\n\t\t</div>\n\n\t\t<img class='img-responsive' src=\"" + __webpack_require__(30) + "\"> </img>\n\t</div>\n\n</div>\n";

/***/ },
/* 27 */
/***/ function(module, exports) {

	module.exports = "data:image/jpeg;base64,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"

/***/ },
/* 28 */
/***/ function(module, exports) {

	module.exports = "data:image/jpeg;base64,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"

/***/ },
/* 29 */
/***/ function(module, exports) {

	module.exports = "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQEASABIAAD/2wBDAAUDBAQEAwUEBAQFBQUGBwwIBwcHBw8LCwkMEQ8SEhEPERETFhwXExQaFRERGCEYGh0dHx8fExciJCIeJBweHx7/2wBDAQUFBQcGBw4ICA4eFBEUHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh7/wAARCABkAYYDASIAAhEBAxEB/8QAHAABAAIDAQEBAAAAAAAAAAAAAAYHAwUIBAIB/8QATxAAAQMDAgMDBggJCQYHAAAAAQACAwQFEQYHEiExE0FhFBciMlGTCBVVVnGB0eEWGCMldZGUlbMnN0JSYnN0scIkQ1RjobQmMzQ1ssHS/8QAGwEBAAIDAQEAAAAAAAAAAAAAAAIDBAUGAQf/xAA8EQABAwICBwILCAMBAQAAAAABAAIRAwQhMQUSQVFhkdETcQYUFRYiU3KBkqGxJDJCUsHh8PEjNGIzNf/aAAwDAQACEQMRAD8A7LRFrr5fLRY4WTXe401Ex5wwzSBvEfAdSpNa551WiSoPe2m0ueYA34LYoot5w9E/OW3e8+5POHon5y273n3K7xO49WeR6LG8o2nrW/EOqlKKLecPRPzlt3vPuTzh6J+ctu959yeJ3HqzyPRPKNp61vxDqpSii3nD0T85bd7z7k84eifnLbvefcnidx6s8j0Tyjaetb8Q6qUoo1T680fUSGODUNBI4Mc8gSdGtBc4/UASsXnE0T85rd7z7lB1Cq0w5pHuK98oWmfat+IdVKkUW84mifnNbve/cnnE0T85rd737lHsn7jyXnlC09a34h1UpRRbziaJ+c1u979yecTRPzmt3vfuTsn7jyTyhaetb8Q6qUoot5xNE/Oa3e9+5fh3E0T85rd7z7k7J+48k8oWnrW/EOqlSKG3HdPb23Vj6Ou1daqeoYGl0b5sEBzQ4d3eCD9a8/ng2y+e1m9/9ytFncESKZ5Hor+2p/mHMKdIoL54NsvntZvf/cnng2y+e1m9/wDcvfErn1buR6L3tqf5hzCnSKC+eDbL57Wb3/3J54NsvntZvf8A3J4lc+rdyPRO2p/mHMKdIoL54NsvntZvf/cpJpnUlh1NROrbBd6K5QNdwvfTSh4afYcdD9KhUtq1Maz2EDiD0XrajHGAQtsiIqVNERERERERERERERERERERERERERERERERERERERERERERERERFzP8IKaabcmpikkc5kEELI2k8mgt4jj6ySumFzNv0M7m1/8AdQ/wwuh8GRN4fZP6Lj/DdxGjh7Q+hVf48Ux4rLw+CcIXeaq+TayxY8Ux4r2wW6unj7SCiqZWf1mQucP1gLHLSzwt4pYJYxnGXMI5/WvJBMSpEOAkgwvNjxTHisvD4Jw+C91VHWWz0gD8aT/4Cr/gPWjGcBSHSLfzpP8A4Cr/AID1og3kuc0qPtHuH6rKc7/Azvd+ix805rJwr9ZG57gxjS5x5AAZJWuVGssXNOa9fkNZ/wAJUe6d9iwlhBIIII6grxenWGYWLmvx2eE/QsvChb6J+herzWUT3bz+H9fz/wB1S/8AbRKKc/apbu2P/H9w/uqX/tolFMeC7Gzb9np9w+i+qtPojuH0C+eftTn7V9Y8F6Bb68gEUFYQeYIp3/YsggDNSXl5+1OftXr+L7h8n1n7O/7Fimp54CBPBLCSMgSRlufoyF5gUxWHn7Vc/wADqrqYN25KWKZzYaq2zdswHk/gLC0nxBJwfEqmseCuD4IY/lki/RtR/oWBpZo8Rqz+Uq+1P+ZneuzkRF8tXUIiIiIiIiIiIiItDrnUsemLVBUijlrqusqo6KipI3Brp55DhreI8mjkSXHoASt8oruTp2uv1ut09plp47naLjDcaQVGRFK9mQY3kZIDmucMgHBwVdbimarRUy/v9YngoVC4NOrmsmn7rqqa+S22+adpaOEUonirKWsdNE55djsjxMaQ4Dn35Ci9q3MulaxtG3TtPJeam71NsoqSKtJY803/AJ00khYOCNvLoCTkDGSskNDrsa0qdVCzxsiNCyBlqffnujdL2g45QOHgaezwBy6g5wSo9Nt5q+Git9xoTTw3r41uVXUMjqg1kVNVnL4mylue1w1oa8NwDk9AFtKdG2k9oW4gRBMA4ztOGU7N21Yrn1Pwzt5YfuvTdN3rvbzd2S6et732mNksro655inhM3ZSzRP7Pm2J+A8EZzxYzjnLr9qu9UeoxaLdardWBtnfcppXVr2CPhOA3AjOQ454T7GuOOS1dn0LNcrjW11/t1FbKF9jNiorXTS9t2VO45kc9+AC4nhAAHIN6klfGg9Kansujru69eS3C/1FKyhiEc+GOghjMcILiOROXvdy6uKVBZxLQJECJzJjjsgzsx2wje22zHTqsVj3PraqawTXS1W23W662aS8S1Lq9zvJoWFuQR2Yy78ozv8Ab4ZmOhrre7zZRcb3ZY7Q+V5MEAnc+Qxf0XPBa3gJ68PMgHng5Cq+27Z3a4v0fbdU2GjntdpsUtrq+C4ZzI7gAla0NGQODIzzBcD1arJ25pdS26xfFOpnxVU1DIYKaubLxPq4AfQkkGPRfw4DuuSM96hfU7VrD2MT37JdEY908IjapUHVS705/gH7wpMiItQstERERERERERERERERFzXvsM7lV/L/dQ/wwulFzbvocbk1/8AdQ/wwuj8F/8AcPsn6hcX4d//ADW+2PoVA+HwVpbEaNob1UVF7usDainpHiOGF4yx8mMkuHeAMcvaVWPEFf3wdqiKTRdVA0/lIq55ePBzWkH/AKH9S6XT9Z9Gyc6mYJIHuK4jwRtqVzpNjaokAEwdpAw6+5eDXW68lhv01ms1sppmUZ7OWSVzgC4dWtDcYA6ZWu3l1RaNQ6DtfkdfSyVbqiOaWmZJl0eY3ZBHgThRa40dqZu1caPUvlDKJ9bNxmLPGeMkxkY7iS1TTcHb/Rum9K1dyzXMnDeCmzMXAykHhBGOnJaxtvYWtW3gHXMEECdaRtx4zgt6+70rf0LyXN7MawIJgtAOwAbQIxOKpLh8Fs7fbqXyF1xuc8kNJ2hjjZE0OlneACQ3PIAAjLj0yBgleDIW5mhkuWmaR1Ix0r7cZW1ETBlzWvdxNkx3t6tJ7sDPULfaRqvo0dZm/PcuGtWhxcSJIEgb/wChjHDvWS1VGnIH1FRDLX0sopZ42xzhsrZC+NzQA5gHCckdRjxUZDBhbChtlfWwzzUlHNNFTsMkr2t9FjQMkk9F5OS5l9R1V0vdJUq1V7mNlsDGIETv/gWLgHtXusFyqLJeaW7UjY3z0r+0YJAS0kA9cLzckOMH6CoEAiCqadV1N4e0wRiF1VqK/Vdu2+n1DDFE6pZRMnax2SziIHjnHNcq1kjqqsmqZA0PmkdI7HTJJJ/zXSutf5l6n9Fxf/Fq5qOFr9HNADjxXbeG9eo6rQYThqT7zmVi4B7ULBg/QsvJPRWxhcPrJqfT2mLtfp7tWXO5zuqY4QIaOJkYi4YWMIc6QHiOWnoAOnMqE6z0o2y08Nyt1Y+utc8hiD5IwyWCTGezkaCRzAJDgcHB6EEKwK63VtC2J1ZSTQNmaHxue3AcCMjB6dFqtfyNtmhn0NWOCrulTBLBC4YeIouIulI6gEuDQe/0sdFstH3lftmUw6RlGGX7Ls9FaVvbi67Gq3CMcIiBgffhnnOCquVuYn4GfRP+S7J1Fuf8QRbfWjT9Rabi67TQUlaO27R0DOGMZAY70XZcevsXHS322waNw9NlrQPztTZwP+a1bfSej6d00PqZMDjG/A/TNdlbV3UiQ3bH1XY2/Ou7lt/pq3XW2U9JO6oucdLKKkOIEZa4uIwRz9FUf8MW52266l09LbLhSVrGUMwc6CdsgaS9pAPCThWF8M/+ba2fphn8KVckNDW+q0D6BhaXwc0fTdSp3QwcC4d8wPkszSNdwc6lsML8wrf+CIP5Yo/0bUf6FUWVb3wRf54o/wBG1H+hb3Sw+w1fZKwbT/3Z3rstQ3VG5+hdMXl9mvt/ioq9jWvdC6GRxw71TlrSOamRXPnwhmg74bZjhBzVNB5df9ojXzjRltTua/Z1JiCcOAJ47l0VzUdTZrN3j5mFP5t7Nr4Z3wy6sp2PjcWvDoJRwkHBz6C29fuToeh03BqOfUlGbVUT9hHUxcUrTJgngPCCQcAnBAVUbewQz/Cy1xDNBHLE6jfxMewOac9hnIP0qsnsjgtW8dJZx+YopoTCGc42uFaAzHd6vEB4ALcM0Na1HhoLhgwnEZPgRlgRPvWG68qtEmM3DkumrBuzt3fbpDbLZqqilrJ3BsUTw+MvcegHGACfBYLnvJttbblU26u1RDDVUsroZozBKSx7Tgjk3uK56q21NwotqKPVdDBZrHhjaO5UbhNNUekzAkBx2Yzw/wBbGSeeFIdWHUrPhSahOkaShqbsLUTFHV+r/wCnjyWjoX5xgHkT1UjoW1DyJMarjmI9F2r96I78MNq88dq6swMwMjtE5T/atyu3l24daxUQ6wggbO58cM3kkrwHtDS70eHngOH61qaXdjR9VUMp6fcunfLIcNb8Uv5nBPs9gWHSelNKVtl/CLsohqi109SbtFTy+hHXzQATiRgy0O8ByHcob8GXRll1htO1l5jnPkN8lngdFIWEONPGw5PeME8lR4pYNpPqHWhpAOW2eGIwHeDK8qdrUe0HaDkSMo3FTQbwaKIBG51Mcjl+aH//AJXtqtytM0tlpLxNuPSikrXvZTEWwl8pYeF2GAcWAeWcYVeaysNv038IbbKwWyN7aKio44YhI7icWh8vU958V6tW7dVenNf2io2sudsmvNqo5ZjZ7jOHy9nJI8mRodyLSXkdQQcYKt8RsTqekRrNLhMRAJEE6uGQxxAlValT0szBj7zuHHFTE7raRFAyuO5NOKd8phDzaX+uGhxGMZ6OBW3drG1t0a3WB19T/EbpOyFV8XcuPi4eHh9bOeXRUTe9Q0Gp9s60TaYpLNdGaxohdm0xJjqJHl4c7BJ4TycCOn614LdTVfxhdNp543vobPea+7SE9DDFTv4G/W4sP1q/yLRLZMgg4g6p9ERJBA4iO9Q18cyZGGLs+fBXZ52tHeT+Uecym7Lj7Pi+KX44sZx6vsX7S7r6PqpTFT7l0z3hjn4Fpf6rWlzj07gCVRcoB+CDA9w5nUpy7HP1HK29CMuEl+tj9xKKz0roqGAaTMDgO0kdARNwnk57uDh4g4YB6Ku40Xa0WOdiYLhm2TqxkNXHPHcBK9p6zyBJxAObts/9LbwbuaKllbGN0aFpccAyWwsH6yMBbTU+vLFpqWljvO4dJTmrpxU07m27tGyREkBwczIIyD3qpvg/0dpq9j9fC8w076Rksri6Vo9AiAFpBPQg4xjvVeVT6yTZHRoqy8xs1HUspuLuj4YyQPDj4/8AqrW6Gtn13UwTDXap+7jLSQRhsjEKJeQwOM4ifvO3xvXSl63J01Z3UrK/camjfVU7KmNjLYXu7J4y1zg0EtBBzzwvNU7raRp4qeWbcunYypi7WIm0u9JnEW56e1pH1KFastuq9Pbk6x1toOpsOoKbg4b1QTuDpaUBgLmObkHGGkjBzjlg4UQ1ZdLTerBtJW2ayQ2al+M54TRxOLmMe2oh4+EnmWknPP2qqhoq2qhhEkHMy3A6pdBBbIywzBGKk8lsiThxdvAmZV6WHX1ivuoG2C17h009zeXBkBtpYXFoyQC4AZxzwtnad09B1c1VQs1TTS1NugfLWOfE+JrGxkNe4lwAHMjlnv5ZVEbq6cu113i11e9PSyxXbTsNDcaZsY5vAY3jx/aAHEPbgjvXis1bQbj0W5lfW3Kj0+LrFbHiarfwwsmafUJ68LntIHfzB7k8j2z6YqaxiGzlILo3DEQZHcVZTrvpOLRnjmSZAnecMQr7od59ta2cwU2po3vDHvA8lmGWsaXOIJZzADSfqWai3e26rbdX3Cl1LDJS0DGSVUgglxG17uFp9Xnk8uSqjS151FYdyNIaO3H0pZ56uGN0FluVKfykLS0s4vRPC4HhwcgHnnCqbT1fqal2xvsVHRSHTDrxF8dTQScMz2EACEf1WkDm72uaOmVKnoK3qTBI+7B1mkEEkYGM8MBhiY4qbr6o3PjsMiAOPHkuqXb1bYsp46h2qoBFI5zGO8nlwS3HEPU7uIfrXv0vuloPU96is1i1DFWV8rXOZC2GRpIaMnm5oHRUt8JF2npNm9FTaThhis76wGkbG3ADTE7ke/iznizzyDlT/QDK47hvm3Fo7NRagZmPTjaR+A+nMQM/Z9HPGcZLxkHOOSwqujrVtr2wDgfSwJE+iYyjH/rHAK5lxVNXUMbNh28Z5b1bqIi59bBFzVvu7G5deP8AlQ/wwulVzNv07G5tw/uof4YXReDH+4fZP1C4zw5E6Ob7Q+hUJ4lJdv8AWNdpC6uqqZgnp5gG1FO44DwOhB7nDng+KinEnEu5rUmVmGnUEgr5ZbVqttVbWpGHDIq77huFtreporjd9P1cldGBgmBpdy5gcQcMgeK1G5m5Vo1VpQWyloq2nqfKWy/lQ3h4W57wevMcsKp+JOJa2loa2pva8T6OUuJAW6r+Et9XpvpkNAeIdDQCe871m4lkpqqelnbPTTSwSt9V8bi1w+gheXiTiW1MHArnwCDIUpsV1uNxucja6vqaoMoastEshcGnsH8wDyUazyW10g786Tf4Cr/gPWkDuXVc3pMBteGiBA/VZdYufRYXGTLs/cs2fFe/T01thvVJLeaeSot7ZM1ETPWe3HQcx/mtVxeKcXitccRCx6ZLHhwGWOOIV91m7Ghqy0PtFVaLo+ifEInRdm3HAMYHr57gqPrHwPq5nUzSyAyOMbT1DcnA/VheTi8U4vFVUaDKM6u1bPSel7nSWr28ejlAA93cs2fFfhPon6Fi4vFHO9E8+5XStVqrU7h6hvtl17dGWi83CgZJHTF7Keocxrj5NFzIBxnx6qDVlVUVtVJVVlTNU1EhzJLNIXvefEnmVI93D/KBcP7ql/7aJRTPiuwsmtFBjgMYH0X1ls6gHAfQL7ypjs/fNKad1lHeNW2yquFLTM7SlZTjLmVDXNLH44mggAHqfqULz4pnxV9amKtM03HA4YYFTY7UcHDYuk90969tdb6Oq7NV2G9zVAY+ShdLE1rYqjgcGPJbJnALufX6FzdnkvnPimfFY9lZUrJhZSmDjiZVlas6sdZ+a+8q3vgiH+WOP9G1H+hU9nxVv/BCP8skX6NqP9Cr0sfsNX2SpWg/zs712eVAda7T6W1dqNl/u8l38tjaxsRgr3xti4ehYB6pzzJHep8i+X0a9Wg7WpOIPBdM+m14hwlVbJsVomS5VNxdPqDyqqLjPILtKHSB3UOIOSPDPctpW7RaIn0ezSkFvqKG1ioFTIykqXxumkAIBkdnL+vQ56D2KfIsg6Ru3RNQ4ZY7suSgLekJ9EKt63ZfRlZpeh05U/G0lDQVD6im4q95fG5zQ0tDuobyGGjkCvNUbGaLnu0t2fVaiFdL687btKJDyAxxZzjAHLKtFEGkrtuVQ7fnns2obakfwhQO27b6b09o+92SgF6fS3Vxlqyyse+pe4gA8L8ggkD28+a8W3WmbBoEVjdP2TVTY6vhMkc7jIwFuebWl2AefM9+B7FZKYHsUTe1nNc17iQ7E4596rfbekCwxHAH6qptT6D0tqHV34U3C0ayFzDmOY+GdzGx8AGAwB3ojl0HtPtWXWmitOar1F+EFfatZUtw8nbT9rQzGD8mCTj0XeJyrUwPYvwjkpt0hXaWkOOAgY5DdlkqzZuMy/P/AJHVU1UbW6DlsFLZGaZ1XT0tPO6pJhlc180pAHHI7i9JwA5eznjqVsajQ+lZ6y6VrrFqxtXdKCO31U7JCJHRMDBkO4shzhG0Od1dzz1K+NaWq6UOtdORyanvbor3fp2SxQ10sLGU/kz3siaA7Aw5gORgnK0dyN1sOuH2Wvuerr7HBZKORxoq+WMds+pfE6V2HZA4Q3OM+qT35WzYa9UAisTIJ27xOfGOSxTSc058Put/mSynaLQhtItJsmtvIRP24g8qdwcfDjPDxY6LNFtZo6Oogn+LtePkp2ubAX10juy4mlp4cv8AR5HuWTdWorbZbLFpmk1rX0l1p6CoqjcTUujfUGNnDE2QDIdxyFvXua7C8u4OpLxe9KaO1Rpe6V1FU1VHPXOp6edzWTuhg7Z0L2dDzY5vt7lNj7yoGntTDicceOOX4o6rx1INkTiI/C3rsSj2f2/p4jT/AIPazlpXPD30z6uTspCOhc0PAK2urtvtG6mjt0FbpnU1NS22DsKSmoyYYYm5JyGB2OLn63UrxP1JWak3Z0vV2681sWn6id1Oymp6gsjqHMpPKXOeB62HSRsx/ZcFcwAx0WJdXV3Qcxz6h1onM4bD7/6VtK21wQHCPZbj81SlXtRo2pqKmd9u1411XjyoMrXgVGAB6fpel07/ABWe8bW6FubbfG/TerKaG3QiGlhppnRsiAcXFwHF65JyXdScexXLgexMD2LH8qXUg65w4+7duVniJ/MPhHVVbpnRundPasl1PQ2vWclxla5szqiodK2UEYw5pd6WO7PRZYtltBy2u7U8dBcKWnvcsVRUwCpczgcx5e1rR/QALjyH0KzcD2IqzpC5nWDyDht3Zctitp2obg8gjuA71W9p2X0bbq2Wv4rzWVr6d9PFVVdxkklp2uaWkxOJ9B2CQD1GeSy6a2f0fp+1Xi10DbmaO70xpquKWtc9paf6QB5B39rqrDReO0hdOBBqHGNu7LkrRb0hk0KqZNgtBSW2C2vN8dSQSPljiNzk4WvdjJA6Z5f5+1bfTO0mlrBqWj1DSz3qor6Nr2wOrLlJO1oc0tIw7wKn6L12krt7S11QkGfnns2oLakDIaEREWErkVP7yba3i/38XyxGGZ8sbY54JJAwgtGA5pPIjGOXgrgRZVneVbOr2lPNYGkdG0NI0OxrZZ4ZyuYvNFrz5Lp/2yP7U80WvPkun/bI/tXTqLb+ct3ubyPVc75kaO3u5jouYvNFrz5Lp/2yP7U80WvPkun/AGyP7V06iect3ubyPVPMjR293MdFzF5otefJdP8Atkf2p5otefJdP+2R/aunUTzlu9zeR6p5kaO3u5joudNO7W61o6+SWe3QNY6kqIgRVMPpPic1vf7SFrRtDrzH/tlP+1x/aunUWHX0vXrv13AT/OKsPgZo8tDZdhO0bfdwXMXmh158mU/7XH9q0epNE6q09EZ7pZ546cdZoyJIx9Lm5x9a65XzIxsjHMe0Oa4Yc0jII9hVTdI1AcQFRV8B7JzCKb3A+4/KAuJuJSyxbdayvNK2qo7LK2F4yx872xBw9oDiCR9Sv6j2y0ZSXwXeGzsEzX9oyMyOMTXe0MPL/wCgpiArKmkfyDmsCx8BsSbt/cG/Ukj5QuYvNDrz5Mp/2uP7UO0OvMH810/T/i4/tXTqKnyhV4LZ+ZOjt7uY6LkLcLY/ci8aurLjQWekkp5Y4Gsc6vjaSWwRsdyJ9rStB+L7up8h0X7xi+1duItlT8JLumwMAbAwyPVb4aLogRJXEf4vu6nyHRfvGL7U/F93U+Q6L94xfau3EU/Oi83N5HqvfJlHeVxH+L7up8h0X7xi+1Pxfd1PkOi/eMX2rtxE86Lzc3keqeTKO8riP8X3dT5Dov3jF9qt74Nuzd90Zf6nU2qHU8NV5O6mpqSGXtC0OILnvcOXcAAM9TlX8iouvCC7uaRpOgA5wP3VlKwpU3BwnBERFo1moiIiIiIiIiIiIiIiKP6l0rR3672i51NdcYJrTOZ6ZtPMGM4yC0lwIOfRLm/Q4ryXLRFLWanqNRR3y+UVdUQMpnmmqWtYImElrQCw4GXOP0kqVor23NVoADshHuOO7eoGm05hRPS2hLdp2sfU0FzvDnPomUXDPUh7WxsLi0jLeTgXPOfa45Xjs22lntU1ufT3S9PZb6mapgilqWuZxTZ7UEcPqu4ncv7RxhThFI3lcknWzz+fDiea87Fm5Qi07ZaetL7B8WT3KljsUkstHHHUDhLpSTIX5bl2Q4t+hTdEVdWtUrGahn+T9VJrGs+6IRERVKSIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIi//9k="

/***/ },
/* 30 */
/***/ function(module, exports) {

	module.exports = "data:image/png;base64,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"

/***/ },
/* 31 */
/***/ function(module, exports) {

	module.exports = "<div class='page-header'>\n\t<h1> Videoer <small>stevner</small></h1>\n</div>\n\n\t<div class='row' id='vidrow1'>\n\t</div>\n";

/***/ },
/* 32 */
/***/ function(module, exports, __webpack_require__) {

	module.exports = "<div class='page-header'>\n\t<h1>Informasjon <small> trening, stevner og vedtekter </small></h1>\n</div>\n\n<div class='row'>\n\t<div class='col-md-6'>\n\t\t<h3>Tren med oss </h3>\n\t\t<p>\t\tVed trening hos oss medbringes treningsklær, \n\t\tjoggesko/crossfitsko/vektløftersko og drikkeflaske!\n\t\t</p>\n\n\t\t<p>\tVi har dame- og herregarderobe. </p>\n\n\t\t<p> Håper vi sees på trening! </p>\n\n\t\t<img class='img-responsive' src='http://gjovikak.no/onewebstatic/312768f14c-IMG_7586.jpg'></img>\n\n\t\t<hr></hr>\n\t\t<p class='lead'>\n\t\tETNINGSLINJER FOR MEDLEMMER I \n\n\t\tGJØVIK ATLETKLUBB OG VÅRE LEIETAKERE\n\t\t</p>\n\t\t<p>1. Den enkelte atlet har ansvaret for å rydde \n\t\tdet utstyret de har brukt tilbake til rett plass. </p>\n\n\t\t<p> 2. Hvis det er noen som ikke har ryddet etter seg, ta det du. \n\t\tDette skaper et mer ålreit treningsmiljø og neste gang kan det være du \n\t\tsom glemmer deg bort. </p>\n\n\t\t<p> 3. Hvis du ser noen som ikke rydder etter seg, si ifra enten til vedkommende\n\t\t, trener eller et styremedlem slik at det blir ryddet. </p>\n\n\t\t<p> 4. Hvis noen av atletene ønsker å ta med en gjest for å overvære \n\t\ttreningen gjelder følgende: </p>\n\t\t<ul>\n\t\t\t<li> Det må avtales med leder eller trener på forhånd </li>\n\n\t\t\t<li> Medbrakt gjest skal forholde seg rolig og ikke blande seg i eller forstyrre treningen   på noen måte. </li>\n\n\t\t\t<li> Gjelder det noen som vil prøve vektløfting, skal det avtale med trener for å finne passende tidspunkt </li>\n\t\t</ul>\n\n\t\t<p>5.Det skal støvsuges minimum en gang i uken og etter behov. Oppsatt støvsugerliste og sjekkliste skal følges. Alle rom, hovedrom, tilbygg og apparatrom, skal støvsuges.</p>\n\n\t\t<p> 6.Kjøkken, stue og soverom skal ryddes hver dag og vaskes en gang i uken og ved behov. Oppsatt vaskeliste og sjekkliste skal følges. Dersom dette ikke blir overholdt vil kjøkkenet bli stengt på ubestemt tid. </p>\n\n\t</div>\n\t<div class='col-md-6'>\n\t\t<div class='panel panel-default'>\n\t\t\t<div class='panel-heading'>\n\t\t\t\t<h3 class='panel-title'><strong> Her finner du oss </strong> </h3>\n\t\t\t</div>\n\t\t\t<div class='panel-body'>\n\t\t\t\t<p>Gjøvik Stadion, Oscar Nissens Gate 2, 2821 Gjøvik</p>\n\t\t\t\t<iframe src=\"https://www.google.com/maps/embed?pb=!1m18!1m12!1m3!1d1946.4134722714152!2d10.680033608845022!3d60.80095306365439!2m3!1f0!2f0!3f0!3m2!1i1024!2i768!4f13.1!3m3!1m2!1s0x4641da1bd58d5549%3A0x1b525b1a35a37601!2sOscar+Nissens+gate+2%2C+2821+Gj%C3%B8vik!5e0!3m2!1sno!2sno!4v1468631342036\" width=\"520\" height=\"450\" frameborder=\"0\" style=\"border:0\" allowfullscreen></iframe>\n\t\t\t\t<br></br>\n\t\t\t\t<img src='" + __webpack_require__(33) + "' class='img-responsive'></img>\n\t\t\t</div>\n\t\t</div>\n\n\n\t\t<div class='panel panel-default'>\n\t\t\t<div class='panel-heading'>\n\t\t\t\t<h3 class='panel-title'><strong> Medlemspriser </strong></h3>\n\t\t\t\t</div>\n\t\t\t<div class='panel-body'>\n\t\t\t\t<table class='table table-striped'>\n\t\t\t\t\t<tr> \n\t\t\t\t\t\t<td> Ungdum </td>\n\t\t\t\t\t\t<td> 350 kr/ar + medlemsavgift </td>\n\t\t\t\t\t</tr>\n\t\t\t\t\t<tr> \n\t\t\t\t\t\t<td> Junior </td>\n\t\t\t\t\t\t<td> 1350 kr/ar + medlemsavgift </td>\n\t\t\t\t\t</tr>\n\t\t\t\t\t<tr> \n\t\t\t\t\t\t<td> Student </td>\n\t\t\t\t\t\t<td> 1350 kr/ar + medlemsavgift </td>\n\t\t\t\t\t</tr>\n\t\t\t\t\t<tr> \n\t\t\t\t\t\t<td> Senior </td>\n\t\t\t\t\t\t<td> 2000 kr/ar + medlemsavgift </td>\n\t\t\t\t\t</tr>\n\t\t\t\t</table>\n\t\t\t</div>\n\t\t</div>\n\t</div>\n</div>\n";

/***/ },
/* 33 */
/***/ function(module, exports) {

	module.exports = "data:image/jpeg;base64,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"

/***/ },
/* 34 */
/***/ function(module, exports) {

	module.exports = "<div class='page-header'>\n\t<h1> Kontakt oss <small> styret </small> </h1>\n</div>\n\n<div class='row'>\n\t<div class='col-md-4'>\n\n\t\t<div class='page-header'>\n\t\t\t<h3> Tor Kristoffer Klethagen <small> LEDER </small></h3>\n\t\t\t<p> Tlf. 95 91 31 79 </p>\n\t\t<p> Mail: leder@gjovikak.no </p>\n\t\t</div>\n\n\n\t\t<div class='page-header'>\n\t\t\t<h3> Gunnar Evang <small> KASSERER</small></h3>\n\t\t\t<p> Tlf: 91 86 09 69 </p>\n\t\t\t<p> Mail: kasserer@gjovikak.no </p>\n\t\t</div>\n\n\t\t<div class='page-header'>\n\t\t\t<h3> Olav Johansen <small> STYREMEDELM </small></h3>\n\t\t\t<p> Tlf: 98 80 43 09 </p>\n\t\t\t<p> Mail: olav-johansen@hotmail.com </p>\n\t\t</div>\n\n\t\t<div class='page-header'>\n\t\t\t<h3> Dag Aleksander Klethagen <small> STYREMEDELM </small></h3>\n\t\t\t<p> Tlf: 98 67 25 60 </p>\n\t\t\t<p> Mail: sk4t3punk3r_dag@hotmail.com </p>\n\t\t</div>\n\n\t\t<div class='page-header'>\n\t\t\t<h3>Jardar Tøn <small> VARA </small></h3>\n\t\t\t<p> Tlf: 91 10 41 59 </p>\n\t\t\t<p> Mail: ?? </p>\n\t\t</div>\n\n\t</div> \n\t<div class='col-md-4'>\n\n\t\t<div class='page-header'>\n\t\t\t<h3> Tor Eric Sivertsen <small> SEKRETÆR </small></h3>\n\t\t\t<p> Tlf: 95 83 61 59 </p>\n\t\t\t<p> Mail: sekreter@gjovikak.no </p>\n\t\t</div>\n\n\t\t<div class='page-header'>\n\t\t\t<h3> Maren Fikse <small> STYREMEDELM </small></h3>\n\t\t\t<p> Tlf: 91 55 75 90 </p>\n\t\t\t<p> Mail: maren-fikse@hotmail.com </p>\n\t\t</div>\n\n\t\t<div class='page-header'>\n\t\t\t<h3> Oystein Robberstad <small> STYREMEDELM </small></h3>\n\t\t\t<p> Tlf: ? </p>\n\t\t\t<p> Mail: styremedlem1@gjovikak.no </p>\n\t\t</div>\n\n\t\t<div class='page-header'>\n\t\t\t<h3> Stig Henning Gillerhaugen <small> VARA </small></h3>\n\t\t\t<p> Tlf: 99 51 16 22 </p>\n\t\t\t<p> Mail: ?? </p>\n\t\t</div>\n\n\t</div>\n\n\t<div class='col-md-4'>\n\t\t<div class='panel panel-default'>\n\t\t\t<div class='panel-heading'>\n\t\t\t\t<h3 class='panel-title'> <strong> Kontakt oss på facebook </strong> </h3>\n\t\t\t</div>\n\t\t\t<div class='panel-body'>\n\t\t\t<iframe src=\"https://www.facebook.com/plugins/page.php?href=https%3A%2F%2Fwww.facebook.com%2FGj%C3%B8vik-Atletklubb-338312802949307%2F&tabs&width=340&height=214&small_header=false&adapt_container_width=true&hide_cover=false&show_facepile=true&appId\" width=\"340\" height=\"214\" style=\"border:none;overflow:hidden\" scrolling=\"no\" frameborder=\"0\" allowTransparency=\"true\"></iframe>\t\n\t\t\t</div>\n\t\t</div>\n\n\t\t<div class='panel panel-default'>\n\t\t\t<div class='panel-heading'>\n\t\t\t\t<h3 class='panel-title'> <strong> Trenere </strong> </h3>\n\t\t\t</div>\n\t\t\t<div class='panel-body'>\n\t\t\t\t<p><strong>Hovedtrener:</strong></p>\n\t\t\t\t<p> Olav Johansen </p>\n\t\t\t\t<p> 98 80 43 09</p>\n\t\t\t\t<br></br>\n\t\t\t\t<p> <strong> Nybegynner Trener:</strong></p>\n\t\t\t\t<p> Olav Johansen </p>\n\t\t\t\t<p> 98 80 43 09</p>\n\t\t\t</div>\n\t\t</div>\n\t</div>\n\n</div>\n\n\n";

/***/ }
/******/ ]);
(function () {
	'use strict';

	angular.module('skyUtils').service('skyUtils', skyUtils);

	skyUtils.$inject = ['$q'];

	function skyUtils($q) {
		var _this = this;

		_this.asyncEach = function asyncEach(arr, cb) {
			var deferreds = [];
	
			angular.forEach(arr, function(item) {
				var deferred = $q.defer();
				
				cb(item).then(function(res) {
					deferred.resolve(res);
				});
				
				deferreds.push(deferred.promise);
			});
			
			return $q.all(deferreds);			
		};

		_this.unserialize = function(string) {
			var str = string.match(/^\?/) ? string.substring(1) : string;
			return str.trim().split('&').reduce(function (ret, param) {
				var parts = param.replace(/\+/g, ' ').split('=');
				var key = parts[0];
				var val = parts[1];

				key = decodeURIComponent(key);
				val = val === undefined ? true : decodeURIComponent(val);

				if (!ret.hasOwnProperty(key)) {
					if(key) {
						ret[key] = val;
					}
				} else if (Array.isArray(ret[key])) {
					ret[key].push(val);
				} else {
					ret[key] = [ret[key], val];
				}

				return ret;
			}, {});
		};

		/* private method used by serialize() - heavily inspired by jQuery buildParams method */
		var buildParams = function buildParams( prefix, obj, add ) {
			var name;

			if ( obj.constructor.toString().indexOf('Array') > -1) {
				// Serialize array item.
				obj.forEach(function( v, i ) {
					if ( /\[\]$/.test( prefix ) ) {
						// Treat each array item as a scalar.
						add( prefix, v );

					} else {
						// Item is non-scalar (array or object), encode its numeric index.
						buildParams(
							prefix + "[" + ( typeof v === "object" ? i : "" ) + "]",
							v,
							add
						);
					}
				});

			} else if ( typeof obj === "object" ) {
				// Serialize object item.
				for ( name in obj ) {
					buildParams( prefix + "[" + name + "]", obj[ name ], add );
				}

			} else {
				// Serialize scalar item.
				add( prefix, obj );
			}
		};

		_this.serialize = function(obj) {
			/* Heavily inspired by jQuery obj-serialize */
			var prefix,
			s = [],
			add = function( key, value ) {
				// If value is a function, invoke it and return its value
				value = typeof value === 'function' ? value() : ( value === null ? "" : value );
				s[ s.length ] = encodeURIComponent( key ) + "=" + encodeURIComponent( value );
			};

			for ( prefix in obj ) {
				buildParams( prefix, obj[ prefix ], add );
			}

			// Return the resulting serialization
			return s.join( "&" ).replace( /%20/g, "+" );
		};

	}

})();
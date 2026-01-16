webpackHotUpdate(0,{

/***/ 657:
/***/ function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(module) {/* REACT HOT LOADER */ if (true) { (function () { var ReactHotAPI = __webpack_require__(78), RootInstanceProvider = __webpack_require__(86), ReactMount = __webpack_require__(88), React = __webpack_require__(153); module.makeHot = module.hot.data ? module.hot.data.makeHot : ReactHotAPI(function () { return RootInstanceProvider.getRootInstances(ReactMount); }, React); })(); } try { (function () {
	
	'use strict';
	
	Object.defineProperty(exports, "__esModule", {
		value: true
	});
	exports.setVerSel = exports.fetchVerified = exports.setDone = exports.postUnverReq = exports.postUnverified = exports.postUnverCompl = exports.setUnverSel = exports.fetchUnverified = exports.recAddr = exports.selAddr = exports.fetchAddr = undefined;
	
	var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };
	
	var _typeof = typeof Symbol === "function" && typeof Symbol.iterator === "symbol" ? function (obj) { return typeof obj; } : function (obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol ? "symbol" : typeof obj; };
	
	var _isomorphicFetch = __webpack_require__(654);
	
	var _isomorphicFetch2 = _interopRequireDefault(_isomorphicFetch);
	
	var _superagent = __webpack_require__(658);
	
	var _superagent2 = _interopRequireDefault(_superagent);
	
	function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }
	
	var reqAddr = function reqAddr(raw) {
		console.log(raw);
		return {
			type: 'REQ_ADDR',
			raw: raw
		};
	};
	
	var recAddr = function recAddr(json) {
		return {
			type: 'REC_ADDR',
			results: json
		};
	};
	
	var selAddr = function selAddr(selected) {
		return {
			type: 'SEL_ADDR',
			selected: selected
		};
	};
	var setDone = function setDone(done) {
		return {
			type: 'SET_DONE',
			done: done
		};
	};
	
	var fetchAddr = function fetchAddr(raw) {
		return function (dispatch) {
			dispatch(reqAddr(raw));
			var eraw = '?address=' + raw.split(' ').join('+') + '&sensor=false';
			var url = 'http://maps.googleapis.com/maps/api/geocode/json';
			console.log(url + eraw);
			return (0, _isomorphicFetch2.default)(url + eraw).then(function (response) {
				return response.json();
			}) //same as function(response){return response.json()}
			.then(function (json) {
				var res = json.results;
				console.log(res);
				console.log(typeof res === 'undefined' ? 'undefined' : _typeof(res));
				if (res.length == 0) {
	
					res.push({ formatted_address: 'none found - search again' });
				}
				dispatch(recAddr(res));
			});
		};
	};
	
	var setUnverSel = function setUnverSel(unver_sel) {
		return {
			type: 'SET_UNVER_SEL',
			unver_sel: unver_sel
		};
	};
	var setVerSel = function setVerSel(idx) {
		return {
			type: 'SET_VER_SEL',
			idx: idx
		};
	};
	
	var recUnverified = function recUnverified(unverified) {
		//console.log(unverified)
		return {
			type: 'REC_UNVERIFIED',
			unverified: unverified
		};
	};
	
	var postUnverCompl = function postUnverCompl(done) {
		return {
			type: 'POST_UNVER_COMPL',
			done: done
		};
	};
	var postUnverReq = function postUnverReq(done) {
		return {
			type: 'POST_UNVER_REQ',
			done: done
		};
	};
	
	var fetchUnverified = function fetchUnverified() {
		return function (dispatch) {
			var url = 'http://10.0.1.106:3036/api/hrs/verify-addr';
			//console.log(url)
			return (0, _isomorphicFetch2.default)(url).then(function (response) {
				return response.json();
			}) //same as function(response){return response.json()}
			.then(function (json) {
				var unverified = json.map(function (j) {
					return _extends({}, j);
				});
				//console.log(unverified)
				dispatch(recUnverified(unverified));
			});
		};
	};
	
	var fetchVerified = function fetchVerified() {
		return function (dispatch) {
			var url = 'http://10.0.1.104:3036/api/hrs/verify-addr/ver';
			//console.log(url)
			return (0, _isomorphicFetch2.default)(url).then(function (response) {
				return response.json();
			}) //same as function(response){return response.json()}
			.then(function (json) {
				var verified = [];
				json.forEach(function (j) {
					verified[j.id] = j;
				});
				dispatch(recVerified(verified));
			});
		};
	};
	
	var recVerified = function recVerified(verified) {
		//console.log(verified)
		return {
			type: 'REC_VERIFIED',
			verified: verified
		};
	};
	
	var postUnverified = function postUnverified(unver_sel) {
		return function (dispatch) {
			var done = false;
			dispatch(postUnverReq(done));
			var id = unver_sel.id;
			var body = _extends({}, unver_sel);
			delete body.isTheDev;
			delete body.isThePlace;
			delete body.idx;
			delete body.id;
			body.veri = 1;
			console.log(id);
			console.log(body);
			var url = 'http://10.0.1.104:3036/api/hrs/verify-addr/' + id;
			return _superagent2.default.put(url).send(body).end(function (e, res) {
				console.log(res.body);
				if (res.body && res.body.affectedRows == 1) {
					done = true;
					console.log(done);
				}
				dispatch(postUnverCompl(done));
			});
			// return {
			// 	type: 'POST_UNVERIFIED',
			// 	done: done
			// }
		};
	};
	
	exports.fetchAddr = fetchAddr;
	exports.selAddr = selAddr;
	exports.recAddr = recAddr;
	exports.fetchUnverified = fetchUnverified;
	exports.setUnverSel = setUnverSel;
	exports.postUnverCompl = postUnverCompl;
	exports.postUnverified = postUnverified;
	exports.postUnverReq = postUnverReq;
	exports.setDone = setDone;
	exports.fetchVerified = fetchVerified;
	exports.setVerSel = setVerSel;
	
	/* REACT HOT LOADER */ }).call(this); } finally { if (true) { (function () { var foundReactClasses = module.hot.data && module.hot.data.foundReactClasses || false; if (module.exports && module.makeHot) { var makeExportsHot = __webpack_require__(185); if (makeExportsHot(module, __webpack_require__(153))) { foundReactClasses = true; } var shouldAcceptModule = true && foundReactClasses; if (shouldAcceptModule) { module.hot.accept(function (err) { if (err) { console.error("Cannot not apply hot update to " + "verify.js" + ": " + err.message); } }); } } module.hot.dispose(function (data) { data.makeHot = module.makeHot; data.foundReactClasses = foundReactClasses; }); })(); } }
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(4)(module)))

/***/ }

})
//# sourceMappingURL=0.1e95f3666e82657dbbd5.hot-update.js.map
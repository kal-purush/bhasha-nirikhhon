webpackHotUpdate(0,{

/***/ 108:
/*!**********************************!*\
  !*** ./app/assets/logo/logo.png ***!
  \**********************************/
/***/ function(module, exports, __webpack_require__) {

	module.exports = __webpack_require__.p + "../Images/logo.png";

/***/ },

/***/ 125:
/*!************************************************!*\
  !*** ./app/components/designerScreen/index.js ***!
  \************************************************/
/***/ function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(module) {/* REACT HOT LOADER */ if (true) { (function () { var ReactHotAPI = __webpack_require__(/*! ./~/react-hot-api/modules/index.js */ 7), RootInstanceProvider = __webpack_require__(/*! ./~/react-hot-loader/RootInstanceProvider.js */ 8), ReactMount = __webpack_require__(/*! react/lib/ReactMount */ 6), React = __webpack_require__(/*! react */ 2); module.makeHot = module.hot.data ? module.hot.data.makeHot : ReactHotAPI(function () { return RootInstanceProvider.getRootInstances(ReactMount); }, React); })(); } try { (function () {
	
	'use strict';
	
	Object.defineProperty(exports, "__esModule", {
	    value: true
	});
	
	var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();
	
	var _react = __webpack_require__(/*! react */ 2);
	
	var React = _interopRequireWildcard(_react);
	
	var _toolbar = __webpack_require__(/*! ./toolbar/toolbar */ 138);
	
	var _toolbar2 = _interopRequireDefault(_toolbar);
	
	var _workspace = __webpack_require__(/*! ./workspace/workspace */ 139);
	
	var _workspace2 = _interopRequireDefault(_workspace);
	
	function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }
	
	function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }
	
	function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }
	
	function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }
	
	function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }
	
	var DesignerScreen = function (_React$Component) {
	    _inherits(DesignerScreen, _React$Component);
	
	    function DesignerScreen(props) {
	        _classCallCheck(this, DesignerScreen);
	
	        return _possibleConstructorReturn(this, (DesignerScreen.__proto__ || Object.getPrototypeOf(DesignerScreen)).call(this, props));
	    }
	
	    _createClass(DesignerScreen, [{
	        key: 'render',
	        value: function render() {
	            return React.createElement(
	                'div',
	                { id: 'designer-screen' },
	                React.createElement(_toolbar2.default, { appCfg: this.props.appCfg }),
	                React.createElement(_workspace2.default, null)
	            );
	        }
	    }]);
	
	    return DesignerScreen;
	}(React.Component);
	
	exports.default = DesignerScreen;
	
	/* REACT HOT LOADER */ }).call(this); } finally { if (true) { (function () { var foundReactClasses = module.hot.data && module.hot.data.foundReactClasses || false; if (module.exports && module.makeHot) { var makeExportsHot = __webpack_require__(/*! ./~/react-hot-loader/makeExportsHot.js */ 9); if (makeExportsHot(module, __webpack_require__(/*! react */ 2))) { foundReactClasses = true; } var shouldAcceptModule = true && foundReactClasses; if (shouldAcceptModule) { module.hot.accept(function (err) { if (err) { console.error("Cannot apply hot update to " + "index.js" + ": " + err.message); } }); } } module.hot.dispose(function (data) { data.makeHot = module.makeHot; data.foundReactClasses = foundReactClasses; }); })(); } }
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(/*! ./../../../~/webpack/buildin/module.js */ 10)(module)))

/***/ },

/***/ 126:
/*!******************************************************************!*\
  !*** ./app/components/designerScreen/searchPanel/searchPanel.js ***!
  \******************************************************************/
/***/ function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(module) {/* REACT HOT LOADER */ if (true) { (function () { var ReactHotAPI = __webpack_require__(/*! ./~/react-hot-api/modules/index.js */ 7), RootInstanceProvider = __webpack_require__(/*! ./~/react-hot-loader/RootInstanceProvider.js */ 8), ReactMount = __webpack_require__(/*! react/lib/ReactMount */ 6), React = __webpack_require__(/*! react */ 2); module.makeHot = module.hot.data ? module.hot.data.makeHot : ReactHotAPI(function () { return RootInstanceProvider.getRootInstances(ReactMount); }, React); })(); } try { (function () {
	
	'use strict';
	
	Object.defineProperty(exports, "__esModule", {
	    value: true
	});
	
	var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();
	
	var _react = __webpack_require__(/*! react */ 2);
	
	var React = _interopRequireWildcard(_react);
	
	var _searchPanelForm = __webpack_require__(/*! ./searchPanelForm */ 130);
	
	var _searchPanelForm2 = _interopRequireDefault(_searchPanelForm);
	
	var _searchPanelFormResult = __webpack_require__(/*! ./searchPanelFormResult */ 131);
	
	var _searchPanelFormResult2 = _interopRequireDefault(_searchPanelFormResult);
	
	var _searchPanelCreateFloor = __webpack_require__(/*! ./searchPanelCreateFloor */ 127);
	
	var _searchPanelCreateFloor2 = _interopRequireDefault(_searchPanelCreateFloor);
	
	var _searchPanelFloorList = __webpack_require__(/*! ./searchPanelFloorList */ 128);
	
	var _searchPanelFloorList2 = _interopRequireDefault(_searchPanelFloorList);
	
	function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }
	
	function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }
	
	function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }
	
	function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }
	
	function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }
	
	var SearchPanel = function (_React$Component) {
	    _inherits(SearchPanel, _React$Component);
	
	    function SearchPanel(props) {
	        _classCallCheck(this, SearchPanel);
	
	        var _this = _possibleConstructorReturn(this, (SearchPanel.__proto__ || Object.getPrototypeOf(SearchPanel)).call(this, props));
	
	        _this.state = {
	            floorList: [{
	                id: 1,
	                name: "Floor-1",
	                width: 12,
	                height: 12
	            }]
	        };
	        return _this;
	    }
	
	    _createClass(SearchPanel, [{
	        key: 'render',
	        value: function render() {
	            return React.createElement(
	                'div',
	                { className: 'mdl-layout__drawer' },
	                React.createElement(
	                    'group',
	                    { className: 'designer-toolbar-group' },
	                    React.createElement(
	                        'span',
	                        { className: 'mdl-layout-title' },
	                        'Select Office'
	                    )
	                ),
	                React.createElement(
	                    'group',
	                    { className: 'designer-toolbar-group' },
	                    React.createElement(_searchPanelForm2.default, null)
	                ),
	                React.createElement(
	                    'group',
	                    { className: 'designer-toolbar-group' },
	                    React.createElement(_searchPanelFormResult2.default, null),
	                    React.createElement(_searchPanelCreateFloor2.default, null),
	                    React.createElement(_searchPanelFloorList2.default, { floorList: this.state.floorList })
	                )
	            );
	        }
	    }]);
	
	    return SearchPanel;
	}(React.Component);
	
	exports.default = SearchPanel;
	
	/* REACT HOT LOADER */ }).call(this); } finally { if (true) { (function () { var foundReactClasses = module.hot.data && module.hot.data.foundReactClasses || false; if (module.exports && module.makeHot) { var makeExportsHot = __webpack_require__(/*! ./~/react-hot-loader/makeExportsHot.js */ 9); if (makeExportsHot(module, __webpack_require__(/*! react */ 2))) { foundReactClasses = true; } var shouldAcceptModule = true && foundReactClasses; if (shouldAcceptModule) { module.hot.accept(function (err) { if (err) { console.error("Cannot apply hot update to " + "searchPanel.js" + ": " + err.message); } }); } } module.hot.dispose(function (data) { data.makeHot = module.makeHot; data.foundReactClasses = foundReactClasses; }); })(); } }
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(/*! ./../../../../~/webpack/buildin/module.js */ 10)(module)))

/***/ },

/***/ 127:
/*!*****************************************************************************!*\
  !*** ./app/components/designerScreen/searchPanel/searchPanelCreateFloor.js ***!
  \*****************************************************************************/
/***/ function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(module) {/* REACT HOT LOADER */ if (true) { (function () { var ReactHotAPI = __webpack_require__(/*! ./~/react-hot-api/modules/index.js */ 7), RootInstanceProvider = __webpack_require__(/*! ./~/react-hot-loader/RootInstanceProvider.js */ 8), ReactMount = __webpack_require__(/*! react/lib/ReactMount */ 6), React = __webpack_require__(/*! react */ 2); module.makeHot = module.hot.data ? module.hot.data.makeHot : ReactHotAPI(function () { return RootInstanceProvider.getRootInstances(ReactMount); }, React); })(); } try { (function () {
	
	"use strict";
	
	Object.defineProperty(exports, "__esModule", {
	    value: true
	});
	
	var _react = __webpack_require__(/*! react */ 2);
	
	var React = _interopRequireWildcard(_react);
	
	function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }
	
	var SearchPanelCreateFloor = function SearchPanelCreateFloor() {
	
	    return React.createElement(
	        "div",
	        { id: "floor-create", className: "mdl-list__item mdl-list__item--two-line" },
	        React.createElement(
	            "div",
	            { className: "mdl-list__item-primary-content" },
	            React.createElement(
	                "form",
	                { id: "floor-create-form" },
	                React.createElement(
	                    "span",
	                    null,
	                    "Floor:"
	                ),
	                React.createElement(
	                    "div",
	                    { className: "mdl-textfield mdl-js-textfield mdl-textfield--floating-label" },
	                    React.createElement("input", { className: "mdl-textfield__input floor-textfield", type: "text", id: "floor-num", pattern: "^[0-9]*" }),
	                    React.createElement(
	                        "label",
	                        { className: "mdl-textfield__label floor-textfield", "for": "floor-num" },
	                        "Number"
	                    )
	                ),
	                React.createElement(
	                    "span",
	                    null,
	                    "Width:"
	                ),
	                React.createElement(
	                    "div",
	                    { className: "mdl-textfield mdl-js-textfield mdl-textfield--floating-label" },
	                    React.createElement("input", { className: "mdl-textfield__input floor-textfield", type: "text", id: "floor-width", pattern: "^[0-9]*" }),
	                    React.createElement(
	                        "label",
	                        { className: "mdl-textfield__label floor-textfield", "for": "floor-width" },
	                        "Meters"
	                    )
	                ),
	                React.createElement(
	                    "span",
	                    null,
	                    "Height:"
	                ),
	                React.createElement(
	                    "div",
	                    { className: "mdl-textfield mdl-js-textfield mdl-textfield--floating-label" },
	                    React.createElement("input", { className: "mdl-textfield__input floor-textfield", type: "text", id: "floor-height", pattern: "^[0-9]*" }),
	                    React.createElement(
	                        "label",
	                        { className: "mdl-textfield__label floor-textfield", "for": "floor-height" },
	                        "Meters"
	                    )
	                )
	            )
	        ),
	        React.createElement(
	            "span",
	            { className: "mdl-list__item-secondary-content" },
	            React.createElement(
	                "button",
	                { id: "add-floor-btn", type: "submit", className: "mdl-button mdl-js-button mdl-button--raised mdl-js-ripple-effect mdl-button--accent mdl-toolbar-button" },
	                React.createElement(
	                    "i",
	                    { className: "material-icons" },
	                    "add"
	                ),
	                "Floor"
	            )
	        )
	    );
	};
	
	exports.default = SearchPanelCreateFloor;
	
	/* REACT HOT LOADER */ }).call(this); } finally { if (true) { (function () { var foundReactClasses = module.hot.data && module.hot.data.foundReactClasses || false; if (module.exports && module.makeHot) { var makeExportsHot = __webpack_require__(/*! ./~/react-hot-loader/makeExportsHot.js */ 9); if (makeExportsHot(module, __webpack_require__(/*! react */ 2))) { foundReactClasses = true; } var shouldAcceptModule = true && foundReactClasses; if (shouldAcceptModule) { module.hot.accept(function (err) { if (err) { console.error("Cannot apply hot update to " + "searchPanelCreateFloor.js" + ": " + err.message); } }); } } module.hot.dispose(function (data) { data.makeHot = module.makeHot; data.foundReactClasses = foundReactClasses; }); })(); } }
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(/*! ./../../../../~/webpack/buildin/module.js */ 10)(module)))

/***/ },

/***/ 128:
/*!***************************************************************************!*\
  !*** ./app/components/designerScreen/searchPanel/searchPanelFloorList.js ***!
  \***************************************************************************/
/***/ function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(module) {/* REACT HOT LOADER */ if (true) { (function () { var ReactHotAPI = __webpack_require__(/*! ./~/react-hot-api/modules/index.js */ 7), RootInstanceProvider = __webpack_require__(/*! ./~/react-hot-loader/RootInstanceProvider.js */ 8), ReactMount = __webpack_require__(/*! react/lib/ReactMount */ 6), React = __webpack_require__(/*! react */ 2); module.makeHot = module.hot.data ? module.hot.data.makeHot : ReactHotAPI(function () { return RootInstanceProvider.getRootInstances(ReactMount); }, React); })(); } try { (function () {
	
	'use strict';
	
	Object.defineProperty(exports, "__esModule", {
	    value: true
	});
	
	var _react = __webpack_require__(/*! react */ 2);
	
	var React = _interopRequireWildcard(_react);
	
	var _searchPanelFloorListItem = __webpack_require__(/*! ./searchPanelFloorListItem */ 129);
	
	var _searchPanelFloorListItem2 = _interopRequireDefault(_searchPanelFloorListItem);
	
	function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }
	
	function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }
	
	var SearchPanelFloorList = function SearchPanelFloorList(props) {
	    var floorList = props.floorList;
	
	
	    return React.createElement(
	        'ul',
	        { id: 'floors-list', className: 'demo-list-action mdl-list' },
	        floorList.map(function (floorItem) {
	            return React.createElement(_searchPanelFloorListItem2.default, { key: floorItem.id, floor: floorItem });
	        })
	    );
	};
	
	exports.default = SearchPanelFloorList;
	
	/* REACT HOT LOADER */ }).call(this); } finally { if (true) { (function () { var foundReactClasses = module.hot.data && module.hot.data.foundReactClasses || false; if (module.exports && module.makeHot) { var makeExportsHot = __webpack_require__(/*! ./~/react-hot-loader/makeExportsHot.js */ 9); if (makeExportsHot(module, __webpack_require__(/*! react */ 2))) { foundReactClasses = true; } var shouldAcceptModule = true && foundReactClasses; if (shouldAcceptModule) { module.hot.accept(function (err) { if (err) { console.error("Cannot apply hot update to " + "searchPanelFloorList.js" + ": " + err.message); } }); } } module.hot.dispose(function (data) { data.makeHot = module.makeHot; data.foundReactClasses = foundReactClasses; }); })(); } }
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(/*! ./../../../../~/webpack/buildin/module.js */ 10)(module)))

/***/ },

/***/ 129:
/*!*******************************************************************************!*\
  !*** ./app/components/designerScreen/searchPanel/searchPanelFloorListItem.js ***!
  \*******************************************************************************/
/***/ function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(module) {/* REACT HOT LOADER */ if (true) { (function () { var ReactHotAPI = __webpack_require__(/*! ./~/react-hot-api/modules/index.js */ 7), RootInstanceProvider = __webpack_require__(/*! ./~/react-hot-loader/RootInstanceProvider.js */ 8), ReactMount = __webpack_require__(/*! react/lib/ReactMount */ 6), React = __webpack_require__(/*! react */ 2); module.makeHot = module.hot.data ? module.hot.data.makeHot : ReactHotAPI(function () { return RootInstanceProvider.getRootInstances(ReactMount); }, React); })(); } try { (function () {
	
	"use strict";
	
	Object.defineProperty(exports, "__esModule", {
	    value: true
	});
	
	var _react = __webpack_require__(/*! react */ 2);
	
	var React = _interopRequireWildcard(_react);
	
	function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }
	
	var SearchPanelFloorListItem = function SearchPanelFloorListItem(props) {
	    var floor = props.floor;
	
	
	    return React.createElement(
	        "li",
	        { id: floor.id, className: "mdl-list__item mdl-list-item-fixed floor-list-item", "data-width": floor.width, "data-height": floor.height },
	        React.createElement(
	            "div",
	            { className: "mdl-list__item-primary-content" },
	            React.createElement(
	                "div",
	                { "data-floor-id": floor.id, className: "floor-edit-btn" },
	                floor.name
	            )
	        ),
	        React.createElement(
	            "button",
	            { "data-floor-id": floor.id, className: "mdl-button mdl-js-button mdl-button--icon mdl-button-color-blue floor-delete-btn" },
	            React.createElement(
	                "i",
	                { className: "material-icons" },
	                "delete"
	            )
	        )
	    );
	};
	
	exports.default = SearchPanelFloorListItem;
	
	/* REACT HOT LOADER */ }).call(this); } finally { if (true) { (function () { var foundReactClasses = module.hot.data && module.hot.data.foundReactClasses || false; if (module.exports && module.makeHot) { var makeExportsHot = __webpack_require__(/*! ./~/react-hot-loader/makeExportsHot.js */ 9); if (makeExportsHot(module, __webpack_require__(/*! react */ 2))) { foundReactClasses = true; } var shouldAcceptModule = true && foundReactClasses; if (shouldAcceptModule) { module.hot.accept(function (err) { if (err) { console.error("Cannot apply hot update to " + "searchPanelFloorListItem.js" + ": " + err.message); } }); } } module.hot.dispose(function (data) { data.makeHot = module.makeHot; data.foundReactClasses = foundReactClasses; }); })(); } }
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(/*! ./../../../../~/webpack/buildin/module.js */ 10)(module)))

/***/ },

/***/ 130:
/*!**********************************************************************!*\
  !*** ./app/components/designerScreen/searchPanel/searchPanelForm.js ***!
  \**********************************************************************/
/***/ function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(module) {/* REACT HOT LOADER */ if (true) { (function () { var ReactHotAPI = __webpack_require__(/*! ./~/react-hot-api/modules/index.js */ 7), RootInstanceProvider = __webpack_require__(/*! ./~/react-hot-loader/RootInstanceProvider.js */ 8), ReactMount = __webpack_require__(/*! react/lib/ReactMount */ 6), React = __webpack_require__(/*! react */ 2); module.makeHot = module.hot.data ? module.hot.data.makeHot : ReactHotAPI(function () { return RootInstanceProvider.getRootInstances(ReactMount); }, React); })(); } try { (function () {
	
	"use strict";
	
	Object.defineProperty(exports, "__esModule", {
	    value: true
	});
	
	var _react = __webpack_require__(/*! react */ 2);
	
	var React = _interopRequireWildcard(_react);
	
	function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }
	
	var SearchPanelForm = function SearchPanelForm() {
	
	    return React.createElement(
	        "form",
	        null,
	        React.createElement(
	            "formgroup",
	            { className: "designer-toolbar-form-group" },
	            React.createElement(
	                "div",
	                { className: "mdl-textfield mdl-js-textfield mdl-textfield--floating-label getmdl-select " },
	                React.createElement("input", { className: "mdl-textfield__input", type: "text", id: "search-country", value: "Poland", readonly: true, tabIndex: "-1" }),
	                React.createElement(
	                    "label",
	                    { "for": "search-country", className: "mdl-textfield__label" },
	                    "Country"
	                ),
	                React.createElement(
	                    "ul",
	                    { "for": "search-country", className: "mdl-menu mdl-menu--bottom-left mdl-js-menu" },
	                    React.createElement(
	                        "li",
	                        { className: "mdl-menu__item" },
	                        "Poland"
	                    ),
	                    React.createElement(
	                        "li",
	                        { className: "mdl-menu__item" },
	                        "Sweden"
	                    )
	                )
	            )
	        ),
	        React.createElement(
	            "formgroup",
	            { className: "designer-toolbar-form-group" },
	            React.createElement(
	                "div",
	                { className: "mdl-textfield mdl-js-textfield mdl-textfield--floating-label getmdl-select " },
	                React.createElement("input", { className: "mdl-textfield__input", type: "text", id: "search-city", value: "Warsaw", readonly: true, tabIndex: "-1" }),
	                React.createElement(
	                    "label",
	                    { "for": "search-city", className: "mdl-textfield__label" },
	                    "City"
	                ),
	                React.createElement(
	                    "ul",
	                    { "for": "search-city", className: "mdl-menu mdl-menu--bottom-left mdl-js-menu" },
	                    React.createElement(
	                        "li",
	                        { className: "mdl-menu__item" },
	                        "Warsaw"
	                    ),
	                    React.createElement(
	                        "li",
	                        { className: "mdl-menu__item" },
	                        "Stockholm"
	                    )
	                )
	            )
	        ),
	        React.createElement(
	            "formgroup",
	            { className: "designer-toolbar-form-group" },
	            React.createElement(
	                "div",
	                { className: "mdl-textfield mdl-js-textfield mdl-textfield--floating-label getmdl-select getmdl-select__fullwidth" },
	                React.createElement("input", { className: "mdl-textfield__input", type: "text", id: "sample1", value: "Office 1, Malborska St. 51", readonly: true, tabIndex: "-1" }),
	                React.createElement(
	                    "label",
	                    { "for": "sample1", className: "mdl-textfield__label" },
	                    "Office"
	                ),
	                React.createElement(
	                    "ul",
	                    { "for": "sample1", className: "mdl-menu mdl-menu--bottom-left mdl-js-menu" },
	                    React.createElement(
	                        "li",
	                        { className: "mdl-menu__item" },
	                        "Office 1, Malborska St. 51"
	                    ),
	                    React.createElement(
	                        "li",
	                        { className: "mdl-menu__item" },
	                        "Office 2, Szwedzki Pl. 3"
	                    )
	                )
	            )
	        ),
	        React.createElement("br", null),
	        React.createElement(
	            "button",
	            { className: "mdl-button mdl-js-button mdl-button--raised mdl-js-ripple-effect mdl-button--accent mdl-long-btn" },
	            React.createElement(
	                "i",
	                { className: "material-icons" },
	                "search"
	            ),
	            "Submit Search"
	        )
	    );
	};
	
	exports.default = SearchPanelForm;
	
	/* REACT HOT LOADER */ }).call(this); } finally { if (true) { (function () { var foundReactClasses = module.hot.data && module.hot.data.foundReactClasses || false; if (module.exports && module.makeHot) { var makeExportsHot = __webpack_require__(/*! ./~/react-hot-loader/makeExportsHot.js */ 9); if (makeExportsHot(module, __webpack_require__(/*! react */ 2))) { foundReactClasses = true; } var shouldAcceptModule = true && foundReactClasses; if (shouldAcceptModule) { module.hot.accept(function (err) { if (err) { console.error("Cannot apply hot update to " + "searchPanelForm.js" + ": " + err.message); } }); } } module.hot.dispose(function (data) { data.makeHot = module.makeHot; data.foundReactClasses = foundReactClasses; }); })(); } }
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(/*! ./../../../../~/webpack/buildin/module.js */ 10)(module)))

/***/ },

/***/ 131:
/*!****************************************************************************!*\
  !*** ./app/components/designerScreen/searchPanel/searchPanelFormResult.js ***!
  \****************************************************************************/
/***/ function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(module) {/* REACT HOT LOADER */ if (true) { (function () { var ReactHotAPI = __webpack_require__(/*! ./~/react-hot-api/modules/index.js */ 7), RootInstanceProvider = __webpack_require__(/*! ./~/react-hot-loader/RootInstanceProvider.js */ 8), ReactMount = __webpack_require__(/*! react/lib/ReactMount */ 6), React = __webpack_require__(/*! react */ 2); module.makeHot = module.hot.data ? module.hot.data.makeHot : ReactHotAPI(function () { return RootInstanceProvider.getRootInstances(ReactMount); }, React); })(); } try { (function () {
	
	"use strict";
	
	Object.defineProperty(exports, "__esModule", {
	    value: true
	});
	
	var _react = __webpack_require__(/*! react */ 2);
	
	var React = _interopRequireWildcard(_react);
	
	function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }
	
	var SearchPanelFormResult = function SearchPanelFormResult() {
	
	    return React.createElement(
	        "div",
	        { id: "search-panel-form-result", className: "mdl-list__item mdl-list__item--two-line" },
	        React.createElement(
	            "span",
	            { className: "mdl-list__item-primary-content" },
	            React.createElement(
	                "i",
	                { className: "material-icons mdl-list__item-avatar" },
	                "business"
	            ),
	            React.createElement(
	                "span",
	                null,
	                "Office 1"
	            ),
	            React.createElement(
	                "span",
	                { className: "mdl-list__item-sub-title" },
	                "Malborska St. 51, Warsaw 03-234"
	            )
	        )
	    );
	};
	
	exports.default = SearchPanelFormResult;
	
	/* REACT HOT LOADER */ }).call(this); } finally { if (true) { (function () { var foundReactClasses = module.hot.data && module.hot.data.foundReactClasses || false; if (module.exports && module.makeHot) { var makeExportsHot = __webpack_require__(/*! ./~/react-hot-loader/makeExportsHot.js */ 9); if (makeExportsHot(module, __webpack_require__(/*! react */ 2))) { foundReactClasses = true; } var shouldAcceptModule = true && foundReactClasses; if (shouldAcceptModule) { module.hot.accept(function (err) { if (err) { console.error("Cannot apply hot update to " + "searchPanelFormResult.js" + ": " + err.message); } }); } } module.hot.dispose(function (data) { data.makeHot = module.makeHot; data.foundReactClasses = foundReactClasses; }); })(); } }
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(/*! ./../../../../~/webpack/buildin/module.js */ 10)(module)))

/***/ },

/***/ 138:
/*!**********************************************************!*\
  !*** ./app/components/designerScreen/toolbar/toolbar.js ***!
  \**********************************************************/
/***/ function(module, exports, __webpack_require__) {

	/* WEBPACK VAR INJECTION */(function(module) {/* REACT HOT LOADER */ if (true) { (function () { var ReactHotAPI = __webpack_require__(/*! ./~/react-hot-api/modules/index.js */ 7), RootInstanceProvider = __webpack_require__(/*! ./~/react-hot-loader/RootInstanceProvider.js */ 8), ReactMount = __webpack_require__(/*! react/lib/ReactMount */ 6), React = __webpack_require__(/*! react */ 2); module.makeHot = module.hot.data ? module.hot.data.makeHot : ReactHotAPI(function () { return RootInstanceProvider.getRootInstances(ReactMount); }, React); })(); } try { (function () {
	
	'use strict';
	
	Object.defineProperty(exports, "__esModule", {
	    value: true
	});
	
	var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();
	
	var _react = __webpack_require__(/*! react */ 2);
	
	var React = _interopRequireWildcard(_react);
	
	var _jquery = __webpack_require__(/*! jquery */ 38);
	
	var _jquery2 = _interopRequireDefault(_jquery);
	
	var _logo = __webpack_require__(/*! ../../../assets/logo/logo.png */ 108);
	
	var _logo2 = _interopRequireDefault(_logo);
	
	var _searchPanel = __webpack_require__(/*! ../searchPanel/searchPanel */ 126);
	
	var _searchPanel2 = _interopRequireDefault(_searchPanel);
	
	function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }
	
	function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }
	
	function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }
	
	function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }
	
	function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }
	
	var Toolbar = function (_React$Component) {
	    _inherits(Toolbar, _React$Component);
	
	    function Toolbar(props) {
	        _classCallCheck(this, Toolbar);
	
	        var _this = _possibleConstructorReturn(this, (Toolbar.__proto__ || Object.getPrototypeOf(Toolbar)).call(this, props));
	
	        _this.debugMode = _this.props.appCfg.debugMode;
	        _this.stageScaleNum = 1;
	
	        _this.state = {
	            stageScaleNumMin: 0.2,
	            stageScaleNumMax: 2,
	            floorData: {}
	        };
	
	        _this.onZoomStage = _this.onZoomStage.bind(_this);
	        _this.onZoomSliderChange = _this.onZoomSliderChange.bind(_this);
	        _this.onZoomInClick = _this.onZoomInClick.bind(_this);
	        _this.onZoomOutClick = _this.onZoomOutClick.bind(_this);
	        _this.onZoomResetClick = _this.onZoomResetClick.bind(_this);
	        _this.onUploadPhotoClick = _this.onUploadPhotoClick.bind(_this);
	        _this.onSaveFloorClick = _this.onSaveFloorClick.bind(_this);
	        _this.onFloorFinderClick = _this.onFloorFinderClick.bind(_this);
	        return _this;
	    }
	
	    /*ZOOM START*/
	
	
	    _createClass(Toolbar, [{
	        key: 'onZoomStage',
	        value: function onZoomStage() {
	
	            var stage = (0, _jquery2.default)('#stage');
	            var stageContainer = (0, _jquery2.default)('#stage-container');
	
	            //TweenLite.killTweensOf(room);
	            //TweenLite.killTweensOf(roomContainer);
	
	            var stageScaleNum = this.stageScaleNum;
	            var _state = this.state,
	                stageScaleNumMin = _state.stageScaleNumMin,
	                stageScaleNumMax = _state.stageScaleNumMax;
	
	
	            if (stageScaleNum < stageScaleNumMin) {
	                stageScaleNum = stageScaleNumMin;
	            }
	            if (stageScaleNum > stageScaleNumMax) {
	                stageScaleNum = stageScaleNumMax;
	            }
	
	            TweenMax.to(stage, 0.3, {
	                scaleX: stageScaleNum,
	                scaleY: stageScaleNum
	            });
	
	            var posX = stageContainer.width() / 2 - stage.width() / 2;
	            var stageWidthAfterScale = stage.width() * stageScaleNum;
	
	            var posY = stageContainer.height() / 2 - stage.height() / 2;
	            var stageHeightAfterScale = stage.height() * stageScaleNum;
	
	            //First for horizontal scale scroll issue
	            //check if scaled room width is bigger than room conatiner
	            //if true align to left
	            if (stageWidthAfterScale >= stageContainer.width()) {
	                TweenMax.set(stage, {
	                    transformOrigin: "0 50%",
	                    x: 0,
	                    y: posY
	                });
	
	                //then check if scaled room height is bigger than room conatiner 
	                //and align to top
	                if (stageHeightAfterScale >= stageContainer.height()) {
	                    TweenMax.set(stage, {
	                        transformOrigin: "0% 0%",
	                        x: 0,
	                        y: 0
	                    });
	                }
	            }
	            //for vertical scale scroll issue
	            //check if scaled room height is bigger than room conatiner 
	            //if true align to top
	            else if (stageHeightAfterScale >= stageContainer.height()) {
	                    TweenMax.set(stage, {
	                        transformOrigin: "50% 0%",
	                        x: posX,
	                        y: 0
	                    });
	
	                    //then check if scaled room width is bigger than room conatiner 
	                    //and align to left
	                    if (stageWidthAfterScale >= stageContainer.width()) {
	                        TweenMax.set(stage, {
	                            transformOrigin: "0 0",
	                            x: 0,
	                            y: 0
	                        });
	                    }
	                }
	                //otherwise appply regular scale with centerd point
	                else {
	                        TweenMax.set(stage, {
	                            transformOrigin: "50% 50%",
	                            x: posX,
	                            y: posY
	                        });
	                    }
	        }
	        /*ZOOM END*/
	
	        /**
	        * Btns Actions START
	        */
	
	        /*SLIDER*/
	
	    }, {
	        key: 'onZoomSliderChange',
	        value: function onZoomSliderChange(evt) {}
	        //if (this.debugMode) console.log("onZoomSliderChange", evt)
	        //this.stageScaleNum = (evt.target.value * 0.1) + 1
	        ////this.onZoomStage();
	        //document.querySelector('#zoom-slider').MaterialSlider.change((this.stageScaleNum - 1) * 10);
	
	
	        /*ZOOM IN BTN*/
	
	    }, {
	        key: 'onZoomInClick',
	        value: function onZoomInClick(evt) {
	            if (this.debugMode) console.log("onZoomIn", evt);
	
	            this.stageScaleNum += 0.1;
	            this.onZoomStage();
	            document.querySelector('#zoom-slider').MaterialSlider.change((this.stageScaleNum - 1) * 10);
	        }
	
	        /*ZOOM OUT BTN*/
	        //$('#zoom-out-floor-btn').click(function () { })
	
	    }, {
	        key: 'onZoomOutClick',
	        value: function onZoomOutClick(evt) {
	            if (this.debugMode) console.log("onZoomOutClick", evt);
	
	            this.stageScaleNum -= 0.1;
	            this.onZoomStage();
	            document.querySelector('#zoom-slider').MaterialSlider.change((this.stageScaleNum - 1) * 10);
	        }
	
	        /*ZOOM RESET BTN*/
	
	    }, {
	        key: 'onZoomResetClick',
	        value: function onZoomResetClick(evt) {
	
	            if (this.debugMode) console.log("onZoomResetClick", evt);
	
	            this.stageScaleNum = 1;
	            this.onZoomStage();
	            document.querySelector('#zoom-slider').MaterialSlider.change((this.stageScaleNum - 1) * 10);
	        }
	    }, {
	        key: 'onUploadPhotoClick',
	
	
	        /*UPLOAD START*/
	        value: function onUploadPhotoClick(evt) {
	
	            if (this.debugMode) console.log("onUploadPhotoClick", evt);
	        }
	
	        /*SAVE START*/
	
	    }, {
	        key: 'onSaveFloorClick',
	        value: function onSaveFloorClick(evt) {
	
	            if (this.debugMode) console.log("onSaveFloorClick", evt);
	
	            //let rooms = [];
	            //$('.item-box').each(function (i, val) {
	
	            //    let itemBox = $(val)
	
	            //    let room = {
	            //        shape: itemBox.attr('data-box-shape'),
	            //        width: itemBox.attr('data-box-w'),
	            //        height: itemBox.attr('data-box-h'),
	            //        xpos: itemBox.attr('data-box-x'),
	            //        ypos: itemBox.attr('data-box-y'),
	            //        rotation: itemBox.attr('data-box-r'),
	            //        floorId: floorCfg.id
	            //    };
	
	            //    rooms.push(room);
	            //});
	
	            //let floor = {
	            //    id: floorCfg.id,
	            //    officeId: floorCfg.officeId,
	            //    name: floorCfg.name,
	            //    width: floorCfg.width,
	            //    height: floorCfg.height,
	            //    xpos: floorCfg.xpos,
	            //    ypos: floorCfg.ypos,
	            //    image: imgPath,
	            //    rooms: rooms
	            //}
	
	            //let action = "/api/floors/" + floorCfg.id;
	            //let data = JSON.stringify(floor);
	
	            //$.ajax({
	            //    contentType: "application/json",
	            //    dataType: 'json',
	            //    type: "PUT",
	            //    url: action,
	            //    data: data,
	            //    cache: false,
	            //    success: function (response) {
	
	            //        if (debugMode) {
	            //            console.log(response);
	            //        }
	
	            //    },
	            //    error: function (xhr, ajaxOptions, thrownError) {
	            //        if (debugMode) {
	            //            console.log(xhr, ajaxOptions, thrownError);
	            //        }
	            //    }
	            //});
	        }
	        /*SAVE END*/
	
	    }, {
	        key: 'onFloorFinderClick',
	        value: function onFloorFinderClick(evt) {
	
	            if (this.debugMode) console.log("onFloorFinderClick", evt);
	            var url = (0, _jquery2.default)(this).data('url');
	
	            //window.location.href = url;
	        }
	
	        /**
	         * Btns Actions END
	         */
	
	    }, {
	        key: 'componentWillMount',
	        value: function componentWillMount() {}
	    }, {
	        key: 'componentDidMount',
	        value: function componentDidMount() {
	            //window.addEventListener("resize", this.updateDimensions);
	            (0, _jquery2.default)('#zoom-slider').on('input', function () {
	
	                stageScaleNum = this.value * 0.1 + 1;
	                ZoomStage();
	            });
	        }
	    }, {
	        key: 'componentWillUnmount',
	        value: function componentWillUnmount() {
	            //window.removeEventListener("resize", this.updateDimensions);
	        }
	    }, {
	        key: 'render',
	        value: function render() {
	            return React.createElement(
	                'div',
	                { id: 'designer-toolbar' },
	                React.createElement(
	                    'div',
	                    { className: 'mdl-layout mdl-js-layout mdl-layout--fixed-header' },
	                    React.createElement(
	                        'header',
	                        { className: 'mdl-layout__header' },
	                        React.createElement(
	                            'div',
	                            { className: 'mdl-layout__header-row' },
	                            React.createElement(
	                                'div',
	                                { className: 'mdl-layout-title logo' },
	                                React.createElement('img', { src: _logo2.default, width: '100' })
	                            ),
	                            React.createElement('div', { className: 'mdl-layout-spacer' }),
	                            React.createElement(
	                                'nav',
	                                { className: 'mdl-navigation mdl-layout--large-screen-only' },
	                                React.createElement(
	                                    'div',
	                                    { id: 'zoom-floor-panel' },
	                                    React.createElement(
	                                        'button',
	                                        {
	                                            id: 'zoom-out-floor-btn',
	                                            onClick: this.onZoomOutClick,
	                                            className: 'mdl-button mdl-js-button mdl-button--fab mdl-js-ripple-effect mdl-button--colored designer-toolbar-zoom-button' },
	                                        React.createElement(
	                                            'i',
	                                            { className: 'material-icons' },
	                                            'zoom_out'
	                                        )
	                                    ),
	                                    React.createElement('input', { id: 'zoom-slider',
	                                        'class': 'mdl-slider mdl-js-slider',
	                                        type: 'range', min: '-10', max: '10', value: '',
	                                        tabindex: '0',
	                                        onchange: 'OnSliderChange(this.value)' }),
	                                    React.createElement(
	                                        'button',
	                                        {
	                                            id: 'zoom-in-floor-btn',
	                                            onClick: this.onZoomInClick,
	                                            className: 'mdl-button mdl-js-button mdl-button--fab mdl-js-ripple-effect mdl-button--colored designer-toolbar-zoom-button' },
	                                        React.createElement(
	                                            'i',
	                                            { className: 'material-icons' },
	                                            'zoom_in'
	                                        )
	                                    )
	                                ),
	                                React.createElement(
	                                    'button',
	                                    {
	                                        id: 'zoom-reset-floor-btn',
	                                        onClick: this.onZoomResetClick,
	                                        className: 'mdl-button mdl-js-button mdl-button--raised mdl-js-ripple-effect mdl-button--accent designer-toolbar-button' },
	                                    React.createElement(
	                                        'span',
	                                        null,
	                                        React.createElement(
	                                            'i',
	                                            { className: 'material-icons' },
	                                            'youtube_searched_for'
	                                        )
	                                    ),
	                                    React.createElement(
	                                        'span',
	                                        null,
	                                        'Reset Zoom'
	                                    )
	                                ),
	                                React.createElement(
	                                    'button',
	                                    {
	                                        id: 'upload-floor-bgnd-btn',
	                                        onClick: this.onUploadPhotoClick,
	                                        className: 'mdl-button mdl-js-button mdl-button--raised mdl-js-ripple-effect mdl-button--accent designer-toolbar-button' },
	                                    React.createElement(
	                                        'span',
	                                        null,
	                                        React.createElement(
	                                            'i',
	                                            { className: 'material-icons' },
	                                            'file_upload'
	                                        )
	                                    ),
	                                    React.createElement(
	                                        'span',
	                                        null,
	                                        'Upload Photo'
	                                    )
	                                ),
	                                React.createElement(
	                                    'button',
	                                    {
	                                        id: 'save-floor-btn',
	                                        onClick: this.onSaveFloorClick,
	                                        className: 'mdl-button mdl-js-button mdl-button--raised mdl-js-ripple-effect mdl-button--accent designer-toolbar-button' },
	                                    React.createElement(
	                                        'span',
	                                        null,
	                                        React.createElement(
	                                            'i',
	                                            { className: 'material-icons' },
	                                            'cloud_upload'
	                                        )
	                                    ),
	                                    React.createElement(
	                                        'span',
	                                        null,
	                                        'Save Floor Plan'
	                                    )
	                                ),
	                                React.createElement(
	                                    'button',
	                                    {
	                                        id: 'floor-finder-btn',
	                                        onClick: this.onFloorFinderClick,
	                                        className: 'mdl-button mdl-js-button mdl-button--raised mdl-js-ripple-effect mdl-button--accent designer-toolbar-button',
	                                        'data-url': '' },
	                                    React.createElement(
	                                        'span',
	                                        null,
	                                        React.createElement(
	                                            'i',
	                                            { className: 'material-icons' },
	                                            'visibility'
	                                        )
	                                    ),
	                                    React.createElement(
	                                        'span',
	                                        null,
	                                        'Floor Finder'
	                                    )
	                                )
	                            )
	                        )
	                    ),
	                    React.createElement(_searchPanel2.default, null)
	                )
	            );
	        }
	    }]);
	
	    return Toolbar;
	}(React.Component);
	
	Toolbar.displayName = "Toolbar";
	
	exports.default = Toolbar;
	
	/* REACT HOT LOADER */ }).call(this); } finally { if (true) { (function () { var foundReactClasses = module.hot.data && module.hot.data.foundReactClasses || false; if (module.exports && module.makeHot) { var makeExportsHot = __webpack_require__(/*! ./~/react-hot-loader/makeExportsHot.js */ 9); if (makeExportsHot(module, __webpack_require__(/*! react */ 2))) { foundReactClasses = true; } var shouldAcceptModule = true && foundReactClasses; if (shouldAcceptModule) { module.hot.accept(function (err) { if (err) { console.error("Cannot apply hot update to " + "toolbar.js" + ": " + err.message); } }); } } module.hot.dispose(function (data) { data.makeHot = module.makeHot; data.foundReactClasses = foundReactClasses; }); })(); } }
	/* WEBPACK VAR INJECTION */}.call(exports, __webpack_require__(/*! ./../../../../~/webpack/buildin/module.js */ 10)(module)))

/***/ }

})
//# sourceMappingURL=0.00e058e735ca90649f20.hot-update.js.map
var __extends = (this && this.__extends) || (function () {
    var extendStatics = Object.setPrototypeOf ||
        ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
        function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var React = require("react");
var ReactDOM = require("react-dom");
var Scene = require('./components/Scene');
var ReactCountdownClock = require('react-countdown-clock');
var numbers = [1, 2, 33, 14, 5, 36, 27, 8, 19, 30, 21, 12, 39, 40, 15, 6, 17, 38, 9, 24];
/*const numbers = [24,4,32,18,11,25,39,];*/
var App = (function (_super) {
    __extends(App, _super);
    function App() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    App.prototype.render = function () {
        return (React.createElement("div", { className: "main-body" },
            React.createElement(Scene, null)));
    };
    return App;
}(React.Component));
ReactDOM.render(React.createElement(App, null), document.getElementById('root'));
//# sourceMappingURL=app.js.map
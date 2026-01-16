var React = require("react");
var App = require('../jsx/app.jsx');

/*

  renderApp
  ---------
  browser version

*/

var renderBrowserApp = function(content, req, res, opts) {
  React.initializeTouchEvents(true);
  React.render(<App content={content} opts={opts}/>, document.getElementById('container'));
};

/*

  isoApp
  --------
  browser version

*/

var isoApp = require("./iso-app")({
  renderApp: renderBrowserApp,
});
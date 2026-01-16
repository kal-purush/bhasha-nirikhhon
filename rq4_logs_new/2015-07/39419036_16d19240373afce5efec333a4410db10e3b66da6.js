var app = require('app');
var BrowserWindow = require('browser-window');

console.log('Electron version: '+process.versions['electron'])

app.on('ready', function() {
  var mainWindow = new BrowserWindow(
	  {width: 800, height: 600}
  );
  mainWindow.openDevTools();
  mainWindow.loadUrl('file://' + __dirname + '/main.html');
});
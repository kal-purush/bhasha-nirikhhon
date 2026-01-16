const {app, BrowserWindow} = require('electron')
const path = require('path')
const url = require('url')

let mainWindow

function createWindow() {
  mainWindow = new BrowserWindow({width: 430, height: 500})

  mainWindow.loadURL(url.format({
    pathname: path.join(__dirname, 'index.html'),
    protocol: 'file:',
    slashes: true
  }))

  mainWindow.on('closed', function() { mainWindow = null })
}

app.on('ready', createWindow)

app.on('window-all-closed', function() { createWindow })
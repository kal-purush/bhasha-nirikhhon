/* eslint-disable import/no-extraneous-dependencies */
const { app, BrowserWindow } = require('electron');
require('electron-reload')(__dirname);

let mainWin;

function createWindow() {
  // Create the window
  mainWin = new BrowserWindow({
    width: 800,
    height: 600,
    webPreferences: {
      nodeIntegration: true,
    },
  });

  // Load main html file
  mainWin.loadURL('http://localhost:3000');

  mainWin.on('close', () => {
    mainWin = null;
  });
}

app.on('ready', createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('activate', () => {
  if (mainWin == null) {
    createWindow();
  }
});
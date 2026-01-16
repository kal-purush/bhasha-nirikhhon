const { app, BrowserWindow } = require('electron');

let mainWindow;

function createMainBroserWindow()
{
    mainWindow = new BrowserWindow({
        width: 800,
        height: 800,
        center: true,
        fullscreenable: false,
        show: process.env.NODE_ENV === "development",
        useCentersize: true
    });
    mainWindow.loadURL(`file://${__dirname}/dist/netease-music.html`);
    mainWindow.on("closed", function() {
        mainWindow = null;
    });
}

app.on('ready', createMainBroserWindow);

app.on('window-all-closed', function () {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});
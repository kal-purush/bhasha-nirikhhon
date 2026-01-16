const { app, BrowserWindow, ipcMain} = require("electron")
const { join } = require("path")
app.whenReady()
    .then( () => {
        const janela = new BrowserWindow({
            frame: false,
            height: 500,
            width: 720,
            icon: join(__dirname, "/public/icon.png"),  
            webPreferences: {
                preload: join(__dirname, "preload.js")
            }         
        })
        
        ipcMain.on("Fechar", () => {app.quit()})
        ipcMain.on("Minimizar", () => {janela.minimize()})
        ipcMain.on("Maximizar", () => {
            janela.isMaximized() ? 
                janela.unmaximize() : janela.maximize()
        })

        janela.loadFile("./public/Reader.html")
    })
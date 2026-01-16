const { app, BrowserWindow, ipcMain, Menu } = require('electron');
const io = require('socket.io-client');
const socket = io('http://127.0.0.1:3000');

let charcoalWindow;

app.on('ready', createCharcoalWindow);
app.on('window-all-closed', function(){
    app.quit();
});

function createCharcoalWindow(){
    charcoalWindow = new BrowserWindow({width: 800, height: 650});
    const menu = Menu.buildFromTemplate(charcoalMenuTemplate);
    Menu.setApplicationMenu(menu);
    charcoalWindow.loadFile('./index.html');

    charcoalWindow.toggleDevTools();

    charcoalWindow.on('closed', function(){
        charcoalWindow = null;
    });
}

const charcoalMenuTemplate = [
    {
        label: 'File',
        submenu: [
            {
                label: 'Register',
                click(){
                    loginWindow.close();
                    createRegisterWindow();
                }
            },
            {
                label: 'Logout',
                click(){
                    ipcMain.emit('logout');
                }
            },
            {
                label: 'Quit',
                accelerator: 'Ctrl+Q',
                click(){
                    app.quit();
                }
            }
        ]
    },
    {
        label: 'Dev',
        submenu: [
            {
                label: 'Dev Tools',
                click(){
                    charcoalWindow.toggleDevTools();
                }
            },
            {
                role: 'reload'
            }
        ]
    }
];

ipcMain.on('request:APIKeys', function(event){
    socket.emit('request:APIKeys');
    socket.on('requested:APIKeys', function(keys){
        event.sender.send('requested:APIKeys', keys);
    })
});
const electron = require ('electron');
const Menu = electron.Menu;
const MenuItem = electron.MenuItem;
const menuSettings = new Menu();

module.exports = {
    fnMakeRightClickMenus: function (mainFunctions, ctxMenu) {
            
 
        ctxMenu.append(new MenuItem( {
            label: 'Start Timer',
            id: 'miStartTimer',
            click: function() {mainFunctions.startStopTimer ('Start');}
        }));

        ctxMenu.append(new MenuItem( {
            label: 'Stop Timer',
            id: 'miStopTimer',
            visible: false,
            click: function() {mainFunctions.startStopTimer('Stop');}
        }));

        ctxMenu.append(new MenuItem( {
            label: 'Login',
            id: 'miLogin',
            click: function() {
                mainFunctions.doLogin();
            }
        })); 

        menuSettings.append(new MenuItem( {
            label: 'Preferences',
            id: 'miPreferences',
            visible: true,
            click: function() {
                mainFunctions.showPreferences();
            }
        }));
     
        menuSettings.append(new MenuItem( {
            label: 'Manage Splits',
            id: 'miManageSplits',
            visible: true,
            click: function() {
                mainFunctions.showSplits();
            }
        }));
    
        ctxMenu.append(new MenuItem( {
            label: 'Settings',
            id: 'miSettings',
            submenu: menuSettings,
            visible: false
        }));
    

        ctxMenu.append(new MenuItem( {
            label: 'Exit',
            id: 'miExit',
            click: function() {
                mainFunctions.closeApp();
            }
        }));

    }

    

}
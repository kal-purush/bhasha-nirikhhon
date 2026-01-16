/// <reference path="../../typings/main.d.ts"/>
/// <reference path="./WindowManager.ts"/>

import Electron = require("electron");

const Menu = Electron.Menu;
const Tray = Electron.Tray;
const nativeImage = Electron.nativeImage;
const wm = require("./WindowManager");

const SUPPORT_URL = "https://github.com/takeo-asai/Electron_app/";

// singleton
class Menubar {
  // class methods
  private static _menubar: Menubar = null;
  public static getMenubar() {
    if (!this._menubar) {
      this._menubar = new Menubar();
    }
    return this._menubar;
  }
  constructor() {
    if (Menubar._menubar) {
      throw new Error("must use the getManager().");
    }
    let iconPath = require("path").normalize(__dirname + "/../../Stock_graph.png"); // `must` normalize icon_path
    let appIcon = new Tray(nativeImage.createFromPath(iconPath));

    let template = [];
    template.push(this.viewMenu());
    template.push(this.helpMenu());
    template.unshift(this.appleMenu());
    let menu = Menu.buildFromTemplate(template);

    appIcon.setContextMenu(menu);
    appIcon.setToolTip("This is sample.");
    Menu.setApplicationMenu(menu);
  }


  // each MenuItem is divided as a function
  private helpMenu() {
    let menu = {
      label: "Help", role: "help",
      submenu: [
        {label: "Learn More", click: () => { Electron.shell.openExternal(SUPPORT_URL); }},
        {label: "Open Data Folder", click: () => { Electron.shell.showItemInFolder(Electron.app.getPath("userData") + "/"); }},
      ]
    };
    return menu;
  }

  private appleMenu() {
    if (process.platform === "darwin") {
      let app = Electron.app;
      let name = app.getName();
      let menu = {
        label: name,
        submenu: [
          {label: "Quit", accelerator: "Command+Q", click: function() { app.quit(); }},
        ]
      };
      return menu;
    }
  }

  private viewMenu() {
    let menu = {
      label: "View",
      submenu: [
        {label: "Reload", accelerator: "CmdOrCtrl+R",
          click: function(item, focusedWindow) {
            if (focusedWindow) {
              focusedWindow.reload();
            }
          }
        },
        {type: "separator"},
        {label: "Toggle visible", click: () => { wm.WindowManager.toggleVisible(); }},
      ]
    };
    return menu;
  }
}
export = Menubar;
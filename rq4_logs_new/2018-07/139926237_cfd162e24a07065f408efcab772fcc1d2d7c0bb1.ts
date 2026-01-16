import { BrowserWindow } from 'electron'

let windowStateKeeper: any | null = null

class AppWindow {
  private window: Electron.BrowserWindow

  private minWidth = 960
  private minHeight = 660

  public constructor() {
    if (!windowStateKeeper) {
      // `electron-window-state` requires Electron's `screen` module, which can
      // only be required after the app has emitted `ready`. So require it
      // lazily.
      windowStateKeeper = require('electron-window-state')
    }

    const savedWindowState = windowStateKeeper({
      defaultWidth: this.minWidth,
      defaultHeight: this.minHeight,
    })

    const windowOptions: Electron.BrowserWindowConstructorOptions = {
      x: savedWindowState.x,
      y: savedWindowState.y,
      width: savedWindowState.width,
      height: savedWindowState.height,
      minWidth: this.minWidth,
      minHeight: this.minHeight,
      show: false,
      backgroundColor: '#fff',
      acceptFirstMouse: true,
    }
    this.window = new BrowserWindow(windowOptions)
    savedWindowState.manage(this.window)
  }
}
export default AppWindow
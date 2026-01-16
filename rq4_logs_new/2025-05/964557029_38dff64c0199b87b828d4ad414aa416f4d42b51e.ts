import { app, BrowserWindow, ipcMain } from 'electron'
import { autoUpdater } from 'electron-updater'
import log from 'electron-log'

// Configure logging
log.transports.file.level = 'info'
autoUpdater.logger = log

// GitHub repository information
const REPO_OWNER = 'ibobdb'
const REPO_NAME = 'sasuai-dekstop'

/**
 * Configure and initialize the auto-updater
 */
export function setupAutoUpdater(mainWindow: BrowserWindow) {
  // Configure updater to use GitHub repository
  autoUpdater.setFeedURL({
    provider: 'github',
    owner: REPO_OWNER,
    repo: REPO_NAME
  })

  // Disable auto download - we'll manually control this
  autoUpdater.autoDownload = false

  // Send update events to the renderer
  autoUpdater.on('checking-for-update', () => {
    mainWindow.webContents.send('update:checking')
  })

  autoUpdater.on('update-available', (info) => {
    mainWindow.webContents.send('update:available', info)
  })

  autoUpdater.on('update-not-available', (info) => {
    mainWindow.webContents.send('update:not-available', info)
  })

  autoUpdater.on('error', (err) => {
    mainWindow.webContents.send('update:error', err)
  })

  autoUpdater.on('download-progress', (progressObj) => {
    mainWindow.webContents.send('update:progress', progressObj)
  })

  autoUpdater.on('update-downloaded', (info) => {
    mainWindow.webContents.send('update:downloaded', info)
  })

  // Handle IPC events from renderer
  ipcMain.handle('update:check', async () => {
    try {
      return await autoUpdater.checkForUpdates()
    } catch (error) {
      log.error('Error checking for updates:', error)
      return null
    }
  })

  ipcMain.handle('update:download', async () => {
    try {
      autoUpdater.downloadUpdate()
      return true
    } catch (error) {
      log.error('Error downloading update:', error)
      return false
    }
  })

  ipcMain.handle('update:install', () => {
    // Quit and install update
    autoUpdater.quitAndInstall(false, true)
    return true
  })

  // Get current app version
  ipcMain.handle('app:getVersion', () => {
    return app.getVersion()
  })

  // Get app name
  ipcMain.handle('app:getName', () => {
    return app.getName()
  })

  // Check for updates automatically on app start (after a short delay)
  setTimeout(() => {
    autoUpdater.checkForUpdates().catch((error) => {
      log.error('Auto update check failed:', error)
    })
  }, 3000)
}
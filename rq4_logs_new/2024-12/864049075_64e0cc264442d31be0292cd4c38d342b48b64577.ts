import { ipcMain } from 'electron'

import { removeDocumentFromPlaylist } from '../db/queries'

export const deleteASongFromPlaylist = () => {
  ipcMain.on('remove-from-playlist', async (event, docId) => {
    try {
      await removeDocumentFromPlaylist(docId)
      event.reply('remove-from-playlist-success', { docId })
    } catch (error) {
      console.error('Error removing document from playlist:', error)
      // event.reply('remove-from-playlist-error', { error: error.message })
    }
  })
}
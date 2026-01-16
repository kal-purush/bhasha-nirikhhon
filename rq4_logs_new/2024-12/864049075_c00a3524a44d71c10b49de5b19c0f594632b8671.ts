import { ipcMain } from 'electron'
import { getRandomSongs } from '../db/queries'

export const getSuggestions = () => {
  ipcMain.handle('suggestion-songs', async () => {
    return await getRandomSongs()
  })
}
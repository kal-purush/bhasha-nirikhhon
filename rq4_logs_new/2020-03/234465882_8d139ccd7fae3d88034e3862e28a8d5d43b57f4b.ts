import { app } from 'electron'
import { promises as fsp } from 'fs'

export const fetchBillingProfiles = () => {
  const appDataPath = app.getPath('userData')
  return fsp.readFile(`${appDataPath}\\testing\\billingProfiles.json`)
    .then(data => {
      const profiles = JSON.parse(data.toString()) // turning buffer into a string and parsing it into JS object
      return { profiles, success: true }
    })
    .catch(err => {
      //TODO: build error handler & ENOENT means file not found / directory not found
      const message = err.code === 'ENOENT' ? 'File not found' : 'unknown err' // leaving it as unknown error for now. Dont want to send the data to frontend
      return { success: false, message }
    })
}
import { ipcMain, app } from 'electron'
import { promises as fsp } from 'fs'

import { BillingProfile } from '@typesTS/billingTypes'

ipcMain.on('startup-data-request', event => {
  const appDataPath = app.getPath('userData')
  // console.log(appDataPath)
  return fsp.readFile(`${appDataPath}\\testing\\billingProfiles.json`)
    .then(data => {
      const profiles = JSON.parse(data.toString()) // turning buffer into a string and parsing it into JS object
      event.sender.send('send-billing-profiles', profiles)
    })
    .catch(err => {
      console.error(err)
      event.sender.send('bad-operation', err)
    })
})

ipcMain.on('update-billing-profile', async (event, payload: BillingProfile) => {
  try {
    const appDataPath = app.getPath('userData')
    const data = await fsp.readFile(`${appDataPath}\\testing\\billingProfiles.json`)
    let profiles = JSON.parse(data.toString())
    profiles = profiles.map((profile: BillingProfile) => {
      if (profile.id === payload.id) return payload
      return profile
    })
    await fsp.writeFile(appDataPath, JSON.stringify(profiles))
    event.sender.send('successful-operation')
  } catch (err) {
    console.error(err)
    event.sender.send('bad-operation', err)
  }
})
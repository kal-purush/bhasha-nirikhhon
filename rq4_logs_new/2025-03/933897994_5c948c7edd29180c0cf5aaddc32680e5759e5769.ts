import { inject } from './injection'

export const runContentScript = async () => {
  const player: HTMLVideoElement = document.getElementById('player0') as HTMLVideoElement
  
  const app = document.getElementById('app')

  if (app && app.childElementCount >= 0) {
    setTimeout(runContentScript, 15000)
    return
  }

  if (!player && !self) {
    setTimeout(runContentScript, 15000)
    return
  }

  console.log('run')
  await inject()
  setTimeout(runContentScript, 15000)
}

runContentScript()
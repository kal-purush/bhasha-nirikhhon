import videos from '@ntp/data'

import { isResourceCached } from './sw'

const currentDay = new Date().getDay()

export const currentVideo = videos[currentDay]

export async function cacheNextVideo() {
  const nextDay = currentDay + 1 > 6 ? 0 : currentDay + 1
  const nextVideo = videos[nextDay]

  if (!currentVideo || !nextVideo) {
    return
  }

  const [isVideoCached, isNextVideoCached] = await Promise.all([
    isResourceCached('videos', currentVideo.url),
    isResourceCached('videos', nextVideo.url),
  ])

  if (!isVideoCached || isNextVideoCached) {
    return
  }

  try {
    await fetch(nextVideo.url, { mode: 'no-cors' })
  } catch (error) {
    console.error(`Failed to cache next video at '${nextVideo.url}'`, error)
  }
}
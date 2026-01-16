import fs from 'node:fs/promises'

import { type Video } from '@ntp/types'
import sampleSize from 'lodash.samplesize'

import { fetchEditorVideos, getPreviewUrl } from './libs/pixabay'

const videos = await fetchEditorVideos()

// Pick 7 random videos (one for each day of the week).
const videosOfTheWeek: Video[] = sampleSize(videos, 7).map((video) => {
  return {
    id: video.id,
    pageUrl: video.pageURL,
    previewUrl: getPreviewUrl(video),
    videoUrl: video.videos.large.size > 0 ? video.videos.large.url : video.videos.medium.url,
  }
})

await fs.rm('dist', { force: true, recursive: true })
await fs.mkdir('dist')

await fs.writeFile('dist/videos.json', JSON.stringify(videosOfTheWeek))
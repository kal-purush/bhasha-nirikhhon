import { Command, Option } from 'commander'
import {
  VIDEO_FILE_EXTENSIONS,
  extensionsToGlobPattern,
} from '@/lib/paths/exts'
import { resolveWslGlob } from '@/lib/fs/glob/resolveWslGlob'
import { promptForMediaGlob } from '@/lib/console/promptForMediaGlob'
import { getVideoStreamsFromContainer } from '@/lib/media-container/video/getVideoStreamsFromContainer'
import { FfprobeStream } from 'fluent-ffmpeg'
import { statSync } from 'fs'

export const moviesDetectLowResolutionsCommand = new Command(
  'detect-low-resolutions'
)
  .addOption(
    new Option('-g, --glob <string>', 'the glob pattern to files to scan')
  )
  .action(async (options: { glob?: string }) => {
    const glob = options.glob ?? (await promptForMediaGlob())
    const allFiles = await resolveWslGlob(
      `${glob}${extensionsToGlobPattern(VIDEO_FILE_EXTENSIONS)}`,
      { nodir: true }
    )

    console.info(
      `${allFiles.length} files found matching the specified pattern`
    )

    let idx = 1
    for (const filePath of allFiles) {
      console.info(`Scanning ${idx} of ${allFiles.length}`)
      console.info(filePath)

      const fileStats = statSync(filePath)
      const fileSizeMB = Math.floor(fileStats.size / (1024 * 1024))
      const bestStream = getBestStream(
        await getVideoStreamsFromContainer(filePath)
      )

      let text: string
      if (bestStream?.width !== undefined || bestStream?.height !== undefined) {
        if (
          (bestStream.width ?? 0 >= 1920) ||
          (bestStream.height ?? 0 >= 1080)
        ) {
          text = 'File is HD'
        } else {
          text = 'File is not HD'
        }

        text += ` (${bestStream.width ?? '?'}x${bestStream.height ?? '?'})`
      } else {
        text = 'File quality is unknown'
      }

      console.info(`${text} (${fileSizeMB})MB`)

      idx++
    }
  })

/**
 * Get the highest resolution stream
 */
const getBestStream = (streams: FfprobeStream[]) =>
  streams.reduce<FfprobeStream | undefined>((best, s) => {
    if (!best) {
      return s
    } else if (best.width === undefined || best.height === undefined) {
      return s
    } else if (s.width === undefined || s.height === undefined) {
      return best
    } else if (s.width > best.width) {
      return s
    } else if (s.height > best.height) {
      return s
    } else {
      return best
    }
  }, undefined)
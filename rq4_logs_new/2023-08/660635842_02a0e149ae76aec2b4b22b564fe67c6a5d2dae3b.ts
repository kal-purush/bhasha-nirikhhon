import aspida from '@aspida/axios'
import * as searchApi from '../../spotify/search/$api'
import { TrackObject } from '../../spotify/@types'
import { Result } from './result'

export const spotify = (accessToken: string) => {
  return {
    // トラックを検索する
    searchTrack: async (
      artist: string,
      song: string
    ): Promise<Result<TrackObject[]>> => {
      const client = searchApi.default(aspida())

      const query = `remaster track:${song} artist:${artist}`.replace(
        / /g,
        '%20'
      )
      const data = await client.get({
        query: { q: query, type: 'track', limit: 20 },
        config: {
          headers: {
            Authorization: `Bearer ${accessToken}`,
          },
        },
      })

      const { body, status } = data

      if (status !== 200) {
        return { data: undefined, error: new Error('search error') }
      }

      const tracks =
        body.tracks.items?.filter((item) => {
          if ((item.album?.artists.length ?? 0) === 0) {
            return false
          }

          const artistName = item.album?.artists[0].name?.toLowerCase() ?? ''
          if (artistName !== artist.toLowerCase()) {
            // 80% of artist name should match
            const matchLength = Math.round(artist.length * 0.8)
            const partialArtistName = artistName.slice(0, matchLength)
            if (!artistName.startsWith(partialArtistName)) {
              return false
            }
          }

          return song.toLowerCase() === item.name?.toLowerCase()
        }) ?? []

      return { data: tracks, error: undefined }
    },
  }
}
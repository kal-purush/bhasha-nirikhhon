import type { Artwork } from '../types/database'
import db from '../utils/database'

const usePopular = () => {
  const result: Array<Artwork & { popularity: number }> = []

  db.favorites.forEach((favorite) => {
    let index = result.findIndex((fav) => fav.id === favorite.artwork_id)

    if (index === -1) {
      const artwork = db.artworks.find((artwork) => artwork.id === favorite.artwork_id)
      if (artwork === undefined) return

      index = result.push({ ...artwork, popularity: 0 }) - 1
    }

    result[index].popularity += 1
  })

  return result.sort((artworkA, artworkB) => artworkB.popularity - artworkA.popularity)
}

export default usePopular
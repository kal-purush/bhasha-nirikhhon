import type { Artwork, Favorite } from '../types/database'
import db from '../utils/database'

type Mode = 'popular' | 'recent'

const getPopular = () => {
  const counts = db.favorites.map((fav) => fav.artwork_id)
    .reduce<{ [artwork_id: Favorite['artwork_id']]: number }>((obj, artwork_id) => {
      obj[artwork_id] = (obj[artwork_id] ?? 0) + 1
      return obj
    }, {})

    return [...db.artworks].sort((artworkA, artworkB) => counts[artworkA.id] - counts[artworkB.id])
}

const getRecent = () => {
  return [...db.artworks].reverse()
}

const useArtworks = (mode?: Mode): Artwork[] => {
  if (mode === 'popular') {
    return getPopular()
  }
  if (mode === 'recent') {
    return getRecent()
  }

  return db.artworks
}

export default useArtworks
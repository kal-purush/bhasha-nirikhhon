import type { Artwork, Favorite } from '../../types/database'
import type { CustomAction } from '../../types/redux'

export enum FavoriteActionTypes {
  ADD_FAVORITE = 'ADD_FAVORITE',
  REMOVE_FAVORITE = 'REMOVE_FAVORITE'
}

export const favoriteAdd = (favoriteData: Omit<Favorite, 'id'> & Partial<Pick<Favorite, 'id'>>): CustomAction<Favorite> => ({
  type: FavoriteActionTypes.ADD_FAVORITE,
  payload: {
    body: favoriteData
  }
})

export const favoriteRemove = (id: Artwork['id']): CustomAction<Favorite> => ({
  type: FavoriteActionTypes.REMOVE_FAVORITE,
  payload: {
    id
  }
})
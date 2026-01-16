import type { Artwork } from '../../types/database'
import type { CustomAction } from '../../types/redux'

export enum ArtworkActionTypes {
  ADD_ARTWORK = 'ADD_ARTWORK',
  EDIT_ARTWORK = 'EDIT_ARTWORK',
  REMOVE_ARTWORK = 'REMOVE_ARTWORK'
}

export const artworkAdd = (artworkData: Artwork): CustomAction<Artwork> => ({
  type: ArtworkActionTypes.ADD_ARTWORK,
  payload: {
    body: artworkData
  }
})

export const artworkEdit = (id: Artwork['id'], artworkData: Partial<Artwork>): CustomAction<Artwork> => ({
  type: ArtworkActionTypes.EDIT_ARTWORK,
  payload: {
    id,
    body: artworkData
  }
})

export const artworkRemove = (id: Artwork['id']): CustomAction<Artwork> => ({
  type: ArtworkActionTypes.REMOVE_ARTWORK,
  payload: {
    id
  }
})
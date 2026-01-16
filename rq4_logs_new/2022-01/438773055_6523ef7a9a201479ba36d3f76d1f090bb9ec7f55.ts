// eslint-disable-next-line import/no-cycle
import { PlaylistObject } from '../api/services/room.services'
import { BadRequest } from '../errors'

type ValidationError = {
  [key: string]: string
}

export const validatePlaylist = (playlist: PlaylistObject): ValidationError => {
  const validRegex =
    /^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$/
  const errors: ValidationError = {}

  if (!playlist.name) {
    errors.username = 'Name your playlist'
  } else if (playlist.name.length < 1 || playlist.name.length > 15) {
    errors.name = 'Name must be between 1 and 15 characters'
  }

  if (!playlist.url) {
    errors.url = 'Please enter a valid Url'
  } else if (
    playlist.url.map((link) => {
      return link.url.match(validRegex)
    })
  ) {
    errors.url = `Url is not valid`
  }

  if (errors.name) {
    throw new BadRequest(errors.name)
  } else if (errors.url) {
    throw new BadRequest(errors.url)
  }

  return errors
}
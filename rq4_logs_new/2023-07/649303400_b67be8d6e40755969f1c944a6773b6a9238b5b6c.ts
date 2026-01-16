import { api, auth } from 'API'
import { PLATFORM } from 'API/constants'
import { handleError } from 'API/error'
import { IMedia } from 'interfaces/media'
import get from 'lodash/get'

export async function uploadMedia(media: IMedia): Promise<IMedia> {
  try {
    const response = await api.post(`/staff/media`, media, {
      headers: auth(PLATFORM.CMS)
    })
    return response.data
  } catch (error) {
    const errorMessage: string = get(error, 'response.data.error.message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/cms/media', 'uploadMedia')
    throw new Error(errorMessage)
  }
}
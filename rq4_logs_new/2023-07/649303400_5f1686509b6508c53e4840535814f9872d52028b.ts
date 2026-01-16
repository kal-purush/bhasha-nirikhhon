import { api, auth } from 'API'
import { PLATFORM } from 'API/constants'
import { handleError } from 'API/error'
import { IBookWithRelations } from 'interfaces/book'
import { get } from 'lodash'

export async function getWebsiteBookDetail(id?: string): Promise<IBookWithRelations> {
  try {
    const response = await api.get(`/staff/books/${id}`, { headers: auth(PLATFORM.WEBSITE) })
    return response.data
  } catch (error) {
    const errorMessage: string = get(error, 'response.data.error.message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/website/book', 'getWebsiteBookDetail')
    throw new Error(errorMessage)
  }
}
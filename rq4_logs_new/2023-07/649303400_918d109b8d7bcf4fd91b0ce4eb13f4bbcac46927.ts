import { api } from 'API'
import { handleError } from 'API/error'
import { AxiosResponse } from 'axios'
import { IOrder } from 'interfaces/order'
import get from 'lodash/get'
import { PaginationList } from 'types'
import { IFilter } from 'types/query'

export async function orderBook(order: IOrder, filter: IFilter<IOrder> = {}): Promise<void> {
  try {
    const response: AxiosResponse = await api.patch(`/customer/orders?filter=${JSON.stringify(filter)}`, order)
    return response?.data
  } catch (error) {
    const errorMessage: string = get(error, 'message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/customer/user', 'orderBook')
    throw new Error(errorMessage)
  }
}

export async function getWebsiteOrderList(filter: IFilter<IOrder> = {}): Promise<PaginationList<IOrder>> {
  try {
    const response: AxiosResponse = await api.get(`/customer/orders/paginate?filter=${JSON.stringify(filter)}`)
    return response?.data
  } catch (error) {
    const errorMessage: string = get(error, 'message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/customer/user', 'orderBook')
    throw new Error(errorMessage)
  }
}
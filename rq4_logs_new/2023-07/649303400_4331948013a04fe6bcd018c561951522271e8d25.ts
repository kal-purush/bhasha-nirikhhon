import { api, auth } from 'API'
import { PLATFORM } from 'API/constants'
import { handleError } from 'API/error'
import { IOrder, IOrderWithRelations } from 'interfaces/order'
import get from 'lodash/get'
import { PaginationList } from 'types'
import { IFilter } from 'types/query'

export async function getCMSOrders(filter: IFilter<IOrder>): Promise<PaginationList<IOrder>> {
  try {
    const response = await api.get(`/staff/orders/paginate?filter=${JSON.stringify(filter)}`, {
      headers: auth(PLATFORM.CMS)
    })
    return response.data
  } catch (error) {
    const errorMessage: string = get(error, 'response.data.error.message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/cms/order', 'getCMSOrders')
    throw new Error(errorMessage)
  }
}

export async function deleteOrderById(id: string): Promise<void> {
  try {
    return api.delete(`/staff/orders/${id}`, {
      headers: auth(PLATFORM.CMS)
    })
  } catch (error) {
    const errorMessage: string = get(error, 'response.data.error.message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/cms/order', 'deleteOrderById')
    throw new Error(errorMessage)
  }
}

export async function createNewOrder(order: IOrder): Promise<IOrder> {
  try {
    const response = await api.post(`/staff/orders`, order, {
      headers: auth(PLATFORM.CMS)
    })
    return response.data
  } catch (error) {
    const errorMessage: string = get(error, 'response.data.error.message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/cms/order', 'createNewOrder')
    throw new Error(errorMessage)
  }
}

export async function updateOrderById(order: IOrder, categoryIds?: string[]): Promise<void> {
  try {
    return api.patch(`/staff/orders/${order?.id}?categoryIds=${categoryIds}`, order, {
      headers: auth(PLATFORM.CMS)
    })
  } catch (error) {
    const errorMessage: string = get(error, 'response.data.error.message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/cms/order', 'updateOrderById')
    throw new Error(errorMessage)
  }
}

export async function getCMSOrderDetail(id: string, filter: IFilter<IOrder>): Promise<IOrderWithRelations> {
  try {
    const response = await api.get(`/staff/orders/${id}?filter=${JSON.stringify(filter)}`, {
      headers: auth(PLATFORM.CMS)
    })
    return response.data
  } catch (error) {
    const errorMessage: string = get(error, 'response.data.error.message', '') || JSON.stringify(error)
    handleError(error as Error, 'API/cms/order', 'getCMSOrderDetail')
    throw new Error(errorMessage)
  }
}
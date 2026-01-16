// api endpoints
import { API_ENDPOINT } from 'src/configs/api'

// axios
import instanceAxios from 'src/helpers/axios'

// types
import {
  TParamsCreateProductType,
  TParamsDeleteMultipleProductType,
  TParamsEditProductType,
  TParamsGetProductTypes
} from 'src/types/product-type'

export const getAllProductTypes = async (data: { params: TParamsGetProductTypes }) => {
  try {
    const res = await instanceAxios.get(`${API_ENDPOINT.MANAGE_PRODUCT.PRODUCT_TYPE.INDEX}`, data)

    return res.data
  } catch (err) {
    return err
  }
}

export const createProductType = async (data: TParamsCreateProductType) => {
  try {
    const res = await instanceAxios.post(`${API_ENDPOINT.MANAGE_PRODUCT.PRODUCT_TYPE.INDEX}`, data)

    return res.data
  } catch (err: any) {
    return err?.response?.data
  }
}

export const updateProductType = async (data: TParamsEditProductType) => {
  const { id, ...rests } = data
  try {
    const res = await instanceAxios.put(`${API_ENDPOINT.MANAGE_PRODUCT.PRODUCT_TYPE.INDEX}/${id}`, rests)

    return res.data
  } catch (err: any) {
    return err?.response?.data
  }
}

export const deleteProductType = async (id: string) => {
  try {
    const res = await instanceAxios.delete(`${API_ENDPOINT.MANAGE_PRODUCT.PRODUCT_TYPE.INDEX}/${id}`)

    return res.data
  } catch (err: any) {
    return err?.response?.data
  }
}

export const deleteMultipleProductType = async (data: TParamsDeleteMultipleProductType) => {
  try {
    const res = await instanceAxios.delete(`${API_ENDPOINT.MANAGE_PRODUCT.PRODUCT_TYPE.INDEX}/delete-many`, { data })
    if (res?.data?.status === 'Success') {
      return {
        data: []
      }
    }

    return {
      data: null
    }
  } catch (err: any) {
    return err?.response?.data
  }
}

export const getDetailsProductType = async (id: string) => {
  try {
    const res = await instanceAxios.get(`${API_ENDPOINT.MANAGE_PRODUCT.PRODUCT_TYPE.INDEX}/${id}`)

    return res.data
  } catch (err: any) {
    return err?.response?.data
  }
}
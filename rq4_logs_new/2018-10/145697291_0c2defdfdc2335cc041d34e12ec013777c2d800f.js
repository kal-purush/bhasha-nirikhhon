import request from '@/utils/request'

export function insertInvoice(params) {
  return request({
    url: '/bf/invoice/insert',
    method: 'POST',
    data: params
  })
}

export function invoiceList(query) {
  return request({
    url: '/bf/invoice/list',
    method: 'get',
    params: query
  })
}

export function updateInvoice(params) {
  return request({
    url: '/bf/invoice/update',
    method: 'POST',
    data: params
  })
}
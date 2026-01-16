import request from '@/utils/request'



export function  getGoods() {
  return request({
    url: '/goods/getGoods',
    method: 'get',
  })
}

export function addGoods(formData) {
  return request({
    url:'/goods/insertGoods',
    method: 'post',
    data:formData,
    headers: {
    'content-type': 'multipart/form-data',
      'processData':false,
      'cache': false
  }
  })
}
export function GoodslList(params) {
  return request({
    url:'/goods/selectGoods',
    method:'post',
    params
  })
}

export function deleteGoods(params) {
  return request({
    url:'/goods/deleteGoods',
    method:'post',
    params
  })
}



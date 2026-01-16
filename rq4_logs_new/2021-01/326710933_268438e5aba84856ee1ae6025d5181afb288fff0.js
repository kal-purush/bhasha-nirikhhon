import request from '@/utils/request'

export const uploadImage = (file) => {
  return request({
    url: '/app/v1_0/user/images',
    method: 'POST',
    data: file
  })
}
import serviceAxios from '../axios'

export const createComment = (data) => {
  return serviceAxios({
    url: '/post/comment',
    method: 'post',
    data
  })
}

export const getCommentList = (id) => {
  return serviceAxios({
    url: `/post/comment/info?id=${id}`,
    method: 'get'
  })
}
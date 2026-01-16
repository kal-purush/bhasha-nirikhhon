import type { addFriendRes, searchFriendResData, friendInfo } from '@/types/friend'
import type { responseData } from '@/types/user'
import request from '@/utils/request'
// 搜索好友
export const friendSearchAPI = (params: { username: string }) => {
  return request<any, searchFriendResData>({
    url: '/friend/search',
    method: 'get',
    params
  })
}

export const addFriendAPI = (data: { id: number; username: string }) => {
  return request<any, addFriendRes>({
    url: '/friend/add_friend',
    method: 'post',
    data
  })
}

// 获取好友信息
//获取好友信息
export const getFriendInfoAPI = (params: any) => {
  return request<any, friendInfo>({
    url: '/friend/info',
    method: 'get',
    params
  })
}
export const updatefriendInfoAPI = (data: any) => {
  return request<any, responseData>({
    url: '/friend/update_friend',
    method: 'post',
    data
  })
}
import api from '../index'

export default {
  // 获取
  list: (data: any) => api.post('template/getHomePageTemplate', data),
  // 新增
  create: (data: any) => api.post('template/insertHomePageTemplate', data),
  // 修改
  edit: (data: any) => api.post('template/updateHomePageTemplate', data,),
  //删除
  del: (data: any) => api.post('template/deleteHomePageTemplate', data),
}
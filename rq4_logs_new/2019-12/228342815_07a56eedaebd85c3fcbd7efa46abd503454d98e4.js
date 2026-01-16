import request from '@/utils/request'

// 编译Python代码
export function runPython (parems) {
  return request({
    url: '/snake_python/python/api/runcode',
    method: 'post',
    params: parems
  })
}
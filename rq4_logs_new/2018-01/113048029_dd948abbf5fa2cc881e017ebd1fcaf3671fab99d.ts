// import { AllProject } from '../db/controllers/index';


// 返回正常数据
const success = ( data: any) => {
  return {
    'state': {
        'code': 1,
        'msg': 'success'
    },
    'data': {
       data
    }
 }
}
// 返回错误提醒
const error = () => {
  return{
    'state': {
        'code': 2,
        'msg':  'error'
    }
  }
}


export const mock = async(ctx: any) => {
  console.log('mock')
  console.log(ctx)
  console.log(ctx.params)
  const method = ctx.request.method.toLowerCase()
  let result
  switch (method) {
    case 'get':
      result = success({'msg': '你调用了get方法'})
      break;
    case 'post':
      result = success({'msg': '你调用了post方法'})
      break;
    case 'put' :
      result = success({'msg': '你调用了put方法'})
      break;
    case 'patch' :
      result = success({'msg': '你调用了patch方法'})
      break;
    case 'delete' :
      result = success({'msg': '你调用了delete方法'})
      break;
    default:
      result = error()
  }


  // console.log(result)
  return ctx.body = result
}
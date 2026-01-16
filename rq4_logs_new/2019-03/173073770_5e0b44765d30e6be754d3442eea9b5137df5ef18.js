import axios from "../libs/api.Axios";
//得到按钮列表数据
export const GetButtionList = (data)=>{
    return axios.request({
        url: '/button/queryButtonList',
        params:data,
        method: 'get'
    })
}
//修改调用接口
export const QueryButtionList = (data)=>{
    return axios.request({
        url: '/button/queryButtonListForTerm',
        params:data,
        method: 'get'
    })
}
//获取菜单列表
export const GetMenuChildList = (data)=>{
    return axios.request({
        url: '/menu/queryMenuChildSelect',
        params:data,
        method: 'get'
    })
}
//修改、新增保存调用接口
export const AddOrModifyButton = (data)=>{
    return axios.request({
        url: '/button/addOrModifyButton',
        data:data,
        method: 'post'
    })
}
//删除接口
export const DeleteButton = (data)=>{
    return axios.request({
        url: `/button/deleteButton?buttonId=${data}`,
        method: 'delete'
    })
}
//获取所有按钮
export  const  getAllButton = (data) =>{
    return axios.request({
        url:'/button/queryAllButtonList',
        params:data,
        method:'get'
    })
}
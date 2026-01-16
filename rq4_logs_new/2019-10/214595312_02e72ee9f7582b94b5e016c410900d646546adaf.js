import axios from 'axios';

let base = '';

// 获取用户列表
export const getUserListPage = (params) => {
    return axios.get(`${base}/user/listpage`, { params });
}
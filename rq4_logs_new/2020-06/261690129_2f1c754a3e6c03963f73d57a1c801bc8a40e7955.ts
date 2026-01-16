import Axios from 'axios';
const axios = Axios.create({
    baseURL: 'http://localhost:3001/',
    timeout: 60000,
    responseType: "json",
    withCredentials: false,
    headers: {
        'Content-Type': 'application/json;charset=UTF-8'
    }
});

// 请求拦截
axios.interceptors.request.use(config => {
    return config;
});

// 响应拦截
axios.interceptors.response.use(res => {
    const data = res.data;
    if(data.code === '000000') {
        data.flag = true;
    }else {
        data.flag = false;
    }
    return data;
}, error => {
    return '服务异常';
});

export default axios;
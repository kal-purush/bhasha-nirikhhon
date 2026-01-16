import axios from 'axios';
const axiosInstance = axios.create({
    // baseURL: process.env.NODE_ENV === "development" ? 'http://flavetechs.com/smp/development/' : "http://flavetechs.com/smp/staging/",
    //    baseURL: 'https://localhost:44373/',
    // baseURL: 'http://flavetechs.com/smp/development/',
    // baseURL: 'http://flavetechs.com/smp/staging/',
    baseURL: 'http://flavetechs.com/all/client/',
    headers: {
        Authorization: '',
    },
});

axiosInstance.interceptors.response.use((response: any) => response, (error: any) => {
    console.log('error.response', error);
    if (!error.response) {
        return;
    }
    if (error.response?.status === 401) {
        localStorage.removeItem('token');
        localStorage.removeItem('permissions');
        localStorage.removeItem('userDetail');
    }
    throw error;
});

axiosInstance.interceptors.response.use(async (response: any) => response, (error: any) => {
    if (!error.response) {
        return;
    }
    if (error?.response?.status === 500) {
        console.log('error.response', error.response)
        return error.response
    }
    if (error?.response?.status === 404) {
        console.log('error.response', error.response)
        return error.response
    }
    throw error;
});

axiosInstance.interceptors.request.use(
    async (config: any) => {
        const sessionToken = await localStorage.getItem('token');
        if (sessionToken) {
            config.headers.Authorization = 'Bearer ' + sessionToken
            return config;
        }
        return config;
    },
    (error: any) => {
        return Promise.reject(error);
    },
);

export default axiosInstance;
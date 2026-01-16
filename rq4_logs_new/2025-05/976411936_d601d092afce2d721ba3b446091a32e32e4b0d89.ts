import axios from 'axios';

const API = axios.create({
    baseURL: import.meta.env.VITE_API_URL,
    headers: {
      'Content-Type': 'application/json'
    }
  });

  API.interceptors.request.use(
    (config) => {
      const user = localStorage.getItem('user');
      const token = user ? JSON.parse(user).token : null;
      if (token) {
        config.headers.Authorization = `Bearer ${token}`;
      }
      return config;
    },
    (error) => Promise.reject(error)
  );

  API.interceptors.response.use(
    (response) => response,
    (error) => {
      const isLoginEndpoint = error.config?.url?.includes('/login');
      if (error.response?.status === 401 && !isLoginEndpoint) {
        localStorage.removeItem('user');
        window.location.href = '/login';
      }
      return Promise.reject(error);
    }
  );

  export default API;
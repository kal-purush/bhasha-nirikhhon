import axios from 'axios';

const api = axios.create({
    baseURL: 'https://nlw-happy.herokuapp.com'
});

export default api;
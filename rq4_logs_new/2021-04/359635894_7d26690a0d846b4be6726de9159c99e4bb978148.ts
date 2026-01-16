import axios from 'axios';

const api = axios.create({
  baseURL: 'https://api.strateegia.digital/',
});

export default api;
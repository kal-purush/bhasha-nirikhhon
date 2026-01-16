import axios from 'axios';
import setupInterceptorsTo from './interceptors';

const baseUrl = axios.create({
  // baseURL: process.env.REACT_APP_BASE_URL,
  baseURL: 'http://huddledown.link/api',
});

export default setupInterceptorsTo(baseUrl);
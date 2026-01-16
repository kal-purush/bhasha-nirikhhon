//import axios from 'axios';
import {create} from 'apisauce';

const api = create({
  baseURL: 'http://192.168.1.107:3000',
});

async function get(path) {
  try {
    let response = await api.get(path);
    return response.data;
  } catch (error) {
    return Promise.reject(error);
  }
}

async function scrapGhStatus() {
  try {
    let response = await get('/scrap/ghstatus');
    return response.message;
  } catch (error) {
    return Promise.reject(error);
  }
}

module.exports = {scrapGhStatus};
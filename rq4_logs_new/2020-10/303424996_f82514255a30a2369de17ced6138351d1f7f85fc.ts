import axios from 'axios';

const api = axios.create({
  baseURL: 'http://192.168.100.75:3333',
});

export default api;

//emulador android => adb reverse tcp:3333 tcp:3333
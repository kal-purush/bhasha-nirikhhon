import axios from 'axios';

const KEY = 'AIzaSyDECzNZ3U2K-OUFLYaJScumlshc0ytdzZg';


export default axios.create({
    baseURL: 'https://www.googleapis.com/youtube/v3',
    params:{
        part: 'snippet',
        maxResult: 5,
        key: KEY
    }
})
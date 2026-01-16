import axios from 'axios';
import keys from '../keys/keys';

export default axios.create({
    baseURL: 'https://api.unsplash.com',
    headers: {
        Authorization: `Client-ID ${keys.accessKey}`,
    }
});
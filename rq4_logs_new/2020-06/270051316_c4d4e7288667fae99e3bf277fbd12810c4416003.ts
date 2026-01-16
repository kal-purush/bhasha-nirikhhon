import axios from 'axios';

const apiItems = axios.create({
     baseURL: "http://localhost:3333/"
});

export default apiItems;
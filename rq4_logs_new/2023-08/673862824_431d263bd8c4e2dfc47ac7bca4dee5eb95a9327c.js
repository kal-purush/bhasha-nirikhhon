import axios from 'axios';

export default axios.create({
	// baseURL: 'https://restaurantchecker-api.vercel.app/api',
	baseURL: 'http://localhost:8080/api',
	headers: { 'Content-Type': 'application/json' },
	withCredentials: true,
});
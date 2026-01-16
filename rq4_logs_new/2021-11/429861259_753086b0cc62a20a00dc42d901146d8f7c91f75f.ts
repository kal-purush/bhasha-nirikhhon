import axios from 'axios';
import store from '../stores';
import { authSliceActions } from '../stores/auth';
import jwtDecode, { JwtPayload } from 'jwt-decode';

const source = axios.CancelToken.source();
const axiosInstance = axios.create({
	baseURL: process.env.NEXT_PUBLIC_BACKEND_URL,
	cancelToken: source.token,
});

interface JwtAuthPayload extends JwtPayload {
	userId: string;
	first_name: string;
}

axiosInstance.interceptors.response.use(response => {
	const tokenPayload = jwtDecode<JwtAuthPayload>(response.headers['x-access-token']);

	store.dispatch(
		authSliceActions.updateAuth({
			accessToken: response.headers['x-access-token'],
			refreshToken: response.headers['x-refresh-token'],
			first_name: tokenPayload.first_name,
			user_id: tokenPayload.userId,
		})
	);

	return response;
});

axiosInstance.interceptors.request.use(request => {
	const authState = store.getState().auth;
	if (request.headers) {
		request.headers.authorization = `Bearer ${authState.accessToken}`;
		request.headers['x-refresh-token'] = authState.refreshToken;
	}

	return request;
});

export { axiosInstance };
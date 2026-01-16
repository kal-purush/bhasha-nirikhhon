import axios from 'axios';
import jwtDecode, { JwtPayload } from 'jwt-decode';
import create from 'zustand';
import { persist } from 'zustand/middleware';

const source = axios.CancelToken.source();
const axiosInstance = axios.create({
	baseURL: process.env.NEXT_PUBLIC_BACKEND_URL,
	cancelToken: source.token,
});

interface AuthState {
	accessToken: string;
	refreshToken: string;
	userId: string;
	first_name: string;
}
interface AuthStore {
	auth: AuthState;
	updateState: (state: Partial<AuthState>) => void;
}
interface JwtAuthPayload extends JwtPayload {
	userId: string;
	first_name: string;
}

const useAuthStore = create<AuthStore>(
	persist(
		set => ({
			auth: {
				accessToken: '',
				refreshToken: '',
				userId: '',
				first_name: '',
			},
			updateState: (authState: Partial<AuthState>) =>
				set(state => ({ auth: { ...state.auth, ...authState } })),
		}),
		{ name: 'authStore' }
	)
);

axiosInstance.interceptors.response.use(response => {
	const tokenPayload = jwtDecode<JwtAuthPayload>(response.headers['x-access-token']);

	useAuthStore.getState().updateState({
		accessToken: response.headers['x-access-token'],
		refreshToken: response.headers['x-refresh-token'],
		first_name: tokenPayload.first_name,
		userId: tokenPayload.userId,
	});

	return response;
});

useAuthStore.subscribe(state => {
	axiosInstance.interceptors.request.use(request => {
		if (request.headers) {
			request.headers['x-access-token'] = state.auth.accessToken;
			request.headers['x-refresh-token'] = state.auth.refreshToken;
		}

		return request;
	});
});

export { axiosInstance, useAuthStore };
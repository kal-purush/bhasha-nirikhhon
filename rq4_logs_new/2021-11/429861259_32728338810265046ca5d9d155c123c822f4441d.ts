import NextAuth from 'next-auth';
import axios from 'axios';
import Providers from 'next-auth/providers';
import jwt, { JwtPayload } from 'jsonwebtoken';

import { axiosServer } from '../../../utils/request';

interface JwtAuthPayload extends JwtPayload {
	user_id: string;
	first_name: string;
}

const options = {
	providers: [
		Providers.Credentials({
			id: 'register',
			name: 'SignUp',
			//@ts-expect-error
			async authorize(credentials) {
				try {
					const response = await axiosServer.post('/auth/signup', {
						first_name: credentials.first_name,
						last_name: credentials.last_name,
						email: credentials.email,
						password: credentials.password,
					});

					if (response) {
						const accessToken = response.headers['x-access-token'];
						const refreshToken = response.headers['x-refresh-token'];
						const payload = jwt.decode(accessToken) as JwtAuthPayload;

						return {
							user: { first_name: payload.first_name, user_id: payload.user_id },
							accessToken,
							refreshToken,
						};
					}

					return null;
				} catch (error) {
					if (axios.isAxiosError(error)) {
						const errorMessage = error?.response?.data?.message || error.message;
						throw new Error(errorMessage);
					}
				}
			},
		}),
		Providers.Credentials({
			id: 'login',
			name: 'SignIn',
			//@ts-expect-error
			async authorize(credentials) {
				try {
					const response = await axiosServer.post('/auth/login', {
						email: credentials.email,
						password: credentials.password,
					});

					if (response) {
						const accessToken = response.headers['x-access-token'];
						const refreshToken = response.headers['x-refresh-token'];
						const payload = jwt.decode(accessToken) as JwtAuthPayload;

						return {
							user: { first_name: payload.first_name, user_id: payload.user_id },
							accessToken,
							refreshToken,
						};
					}

					return null;
				} catch (error) {
					if (axios.isAxiosError(error)) {
						const errorMessage = error.response?.data?.message || error.message;
						throw new Error(errorMessage);
					}
				}
			},
		}),
	],
	callbacks: {
		async signIn(parameters: any) {
			if (parameters.accessToken) {
				return true;
			}

			return false;
		},

		async jwt(token: any, user: any) {
			if (user) {
				token.user = user;
				token.accessToken = user.accessToken;
				token.refreshToken = user.refreshToken;
			}

			return token;
		},

		async session(session: any, token: any) {
			session.user = {
				...token.user.user,
				accessToken: token.user.accessToken,
				refreshToken: token.user.refreshToken,
			};

			return session;
		},
	},
	pages: { signIn: '/auth/login', error: '/auth/login' },
};

const NextAuthConfig = (req: any, res: any) => NextAuth(req, res, options);

export default NextAuthConfig;
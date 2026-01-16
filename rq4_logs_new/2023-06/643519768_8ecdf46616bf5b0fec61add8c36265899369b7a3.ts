import axios from 'axios';
import Cookies from 'js-cookie';

import { IAuthResponse } from '@/store/user/user.interface';

import { getContentType } from '@/api/api.helper';

import { EnumNameToken } from './auth.enums';
import { saveToStorage } from './auth.helper';

export const AuthService = {
	async getNewTokens() {
		const refreshToken = Cookies.get(EnumNameToken.REFRESH);

		const response = await axios.post<string, { data: IAuthResponse }>(
			process.env.SERVER_URL + '/auth/login/access-token',
			{ refreshToken },
			{
				headers: getContentType(),
			},
		);

		if (response.data.accessToken) saveToStorage(response.data);
	},
};

export default AuthService;
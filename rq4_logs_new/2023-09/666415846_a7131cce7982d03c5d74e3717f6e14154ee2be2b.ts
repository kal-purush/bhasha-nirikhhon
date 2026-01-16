import { API_ENDPOINT, ApiClient } from '@/core/api/api-client';

const REDIRECT_URI = window.location.origin;

const client = new ApiClient(API_ENDPOINT);

type OAuthServiceIdReq = {
	redirect_uri: string;
};

type OAuthServiceIdRes = {
	service_id: string;
};

const apiOAuth = {
	async getServiceId() {
		try {
			const response = await client.get<OAuthServiceIdReq, OAuthServiceIdRes>(
				'/oauth/yandex/service-id',
				{ redirect_uri: REDIRECT_URI },
			);
			return response?.data.service_id;
		} catch (error) {
			console.error(error);
		}
	},
	async loginOAuth(code: string) {
		try {
			const response = await client.post('/oauth/yandex', {
				code,
				redirect_uri: `${REDIRECT_URI}`,
			});
			return response;
		} catch (error) {
			console.error(error);
		}
	},
};

export default apiOAuth;
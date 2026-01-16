import { computed, unref } from "vue";
import { createAuth0 } from "@auth0/auth0-vue";
import { apiConfig } from "@/api.ts";

export const auth0 = createAuth0({
	domain: import.meta.env.VITE_AUTH0_DOMAIN,
	clientId: import.meta.env.VITE_AUTH0_CLIENT_ID,
	authorizationParams: {
		redirect_uri: window.location.origin,
		audience: import.meta.env.VITE_AUTH0_AUDIENCE,
	},
});

export const user = computed(() => unref(auth0.user));

export const isAuthenticated = computed(() => unref(auth0.isAuthenticated));

export async function setAccessToken() {
	if (isAuthenticated.value) {
		const token = await auth0.getAccessTokenSilently();
		apiConfig.baseOptions.headers.authorization = `Bearer ${token}`;
	}
}

export async function checkSession() {
	await auth0.checkSession();
	await setAccessToken();
}

export async function logOut() {
	return auth0.logout({
		logoutParams: {
			returnTo: window.location.origin,
		},
	});
}
import type { UserWithoutPassword } from '$lib/api/generated';
import envConstants from '$lib/constants/env.constants';
import authStore from '$lib/stores/auth.store';
import { redirect, type Handle } from '@sveltejs/kit';

// This runs on EVERY server-side request
export const handle: Handle = async ({ event, resolve }) => {
	console.log('Authorization handle');
	const isProtectedRoute = event.route.id?.startsWith('/(protected)');
	const accessToken = event.cookies.get('accessToken');

	if (isProtectedRoute) {
		if (accessToken && !authStore.isUser()) {
			const response = await fetch(`${envConstants.API_URL}/user`, {
				method: 'GET',
				credentials: 'include',
				headers: {
					'Content-Type': 'application/json',
					Cookie: `accessToken=${accessToken}` // ✅ Manually forward cookies
				}
			});

			if (response.status === 200) {
				const user = (await response.json()) as UserWithoutPassword;
				authStore.setUser(user);
			} else {
				throw redirect(303, '/login');
			}
		}

		if (!accessToken) {
			authStore.removeUser();
			throw redirect(303, '/login');
		}
	}

	const response = await resolve(event);
	return response;
};
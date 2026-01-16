import type { UserWithoutPassword } from '$lib/api/generated';
import envConstants from '$lib/constants/env.constants';
import authStore from '$lib/stores/auth.store';
import type { PageServerLoad } from './$types';
import { redirect } from '@sveltejs/kit';

//For authorization we use +page.ts file
export const load: PageServerLoad = async ({ fetch }) => {
	const response = await fetch(`${envConstants.API_URL}/user`, {
		method: 'GET',
		credentials: 'include'
	});

	if (response.status === 200) {
		const user = (await response.json()) as UserWithoutPassword;
		authStore.setUser(user);
	} else {
		throw redirect(303, '/login');
	}
};
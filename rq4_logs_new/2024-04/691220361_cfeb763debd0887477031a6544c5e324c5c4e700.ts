import { authGuard } from '$lib/authGuard/authGuardConfig';
import { createFileSchema } from '$lib/schema/fileSchema.js';
import { fileFormActions } from '$lib/server/fileFormActions';
import { superValidate } from 'sveltekit-superforms';
import { zod } from 'sveltekit-superforms/adapters';
import { dropdownItems } from '$lib/server/dropdownItems.js';

export const load = async (data) => {
	authGuard(data);

	return {
		form: await superValidate(zod(createFileSchema)),
		dropdownInfo: dropdownItems({ db: data.locals.db })
	};
};

export const actions = fileFormActions;
import { redirect } from '@sveltejs/kit';

export async function load({ params }) {
	throw redirect(307, `https://pokemon.helbling.uk/ability/${params.abilityId}`);
}
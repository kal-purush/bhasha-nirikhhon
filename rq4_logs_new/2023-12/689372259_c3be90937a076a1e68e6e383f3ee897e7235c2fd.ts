import { getTagsByUser, type ITags } from '$lib/pb/tags';
import { writable } from 'svelte/store';
import { currentUser } from './user';
import { error } from '$lib/log';
import { addNotification } from './notifications';
import type { ITagPokemon, ITagPokemonNew } from '$lib/types/ITags';
import type { IDisplayPokemon } from './pokemonPageStore';

export const tagStore = writable<ITags[]>([]);

export async function refetchTags(userId: string) {
	try {
		tagStore.set(await getTagsByUser(userId));
	} catch (err) {
		error(
			'Failed to get tags for user',
			'FailedToGetTagsByUser',
			`User: ${userId}, ${JSON.stringify(err)}`
		);
		addNotification({ message: 'Failed to get tags for user', level: 'failure' });
	}
}

export async function createTag(
	userId: string,
	name: string,
	isPrivate: boolean,
	initialContent?: {
		pokemon?: ITagPokemon[];
	}
) {
	try {
		const actualInitialContent = {
			pokemon: initialContent?.pokemon ?? []
		};
		const payload = {
			name,
			isPrivate,
			initialContent: actualInitialContent
		};

		const response = await fetch('/api/tags', {
			method: 'POST',
			body: JSON.stringify(payload)
		});
		if (!response.ok) {
			throw new Error(`${response.status}, ${JSON.stringify(payload)}`);
		}
		addNotification({
			message: `Created new ${isPrivate ? 'private' : ''} tag "${name}"`,
			level: 'success'
		});
		await refetchTags(userId);
	} catch (err) {
		addNotification({ message: 'Failed to create tag', level: 'failure' });
		error(
			'Failed to create new tag',
			'FailedToCreateTag',
			`New Name: ${name}, Private: ${isPrivate}, Error: ${JSON.stringify(err)}`
		);
	}
}

export async function addPokemonToTag(pokemon: ITagPokemonNew, tagId: string) {
	try {
		await fetch(`/api/tag`, {
			method: 'POST',
			body: JSON.stringify({
				id: tagId,
				contents: {
					pokemon: [
						{
							...pokemon,
							added: new Date().toISOString()
						}
					]
				}
			})
		});
	} catch (err) {
		addNotification({ message: 'Could not add tag. Please try again', level: 'failure' });
		error(
			'Failed to add item to tag',
			'FailedToAddToTag',
			`Tag ID: ${tagId}, Pokemon: ${pokemon.id}, Error: ${JSON.stringify(err)}`
		);
	}
}

export async function removePokemonFromTag(pokemon: ITagPokemonNew, tagId: string) {
	try {
		await fetch('/api/tag', {
			method: 'DELETE',
			body: JSON.stringify({
				id: tagId,
				contents: {
					// make sure to be specific on variety/gender/shiny as well on backend
					pokemon: [{ id: pokemon.id }]
				}
			})
		});
	} catch (err) {
		addNotification({ message: 'Could not remove tag. Please try again', level: 'failure' });
		error(
			`Failed to remove item from tag'`,
			'FailedToRemoveFromTag',
			`Tag ID: ${tagId}, Pokemon: ${pokemon.id}, Error: ${JSON.stringify(err)}`
		);
	}
}

export function doesTagContainPokemon(pokemon: IDisplayPokemon, tag: ITags) {
	const isDisplayOnFemale = pokemon.hasFemaleSprite && pokemon.showFemaleSpriteIfExists;

	return tag.contents.pokemon.some((a) => {
		return (
			a.id === pokemon.id &&
			a.shiny === (pokemon.hasShinySprite && pokemon.showShinySpriteIfExists) &&
			((a.gender === 'female' && isDisplayOnFemale) ||
				(a.gender === undefined && !isDisplayOnFemale))
		);
	});
}

currentUser.subscribe(async (user) => {
	tagStore.set([]);
	if (user) {
		await refetchTags(user.id);
	}
});
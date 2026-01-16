import type { IPokemon } from '$lib/types/IPokemon';
import { Generations, type IGeneration } from './games';

interface IType {
	name: string;
	icon: string;
}

export const getPokemonTypesInGame = (pokemon: IPokemon, generation?: IGeneration): IType[] => {
	const vanillaTypes = pokemon.types.map((typeEntry) => {
		return {
			name: typeEntry.type.name,
			icon: `/icons/types/${typeEntry.type.name}.webp`
		};
	});

	if (pokemon.past_types.length === 0 || !generation) {
		return vanillaTypes;
	}

	const currentGenIndex = Generations.findIndex((gen) => {
		return gen.pokeApiName === generation.pokeApiName;
	});

	const pastTypes = pokemon.past_types
		.map((pastType) => {
			const genIndex = Generations.findIndex((gen) => {
				return gen.pokeApiName === pastType.generation.name;
			});

			if (currentGenIndex <= genIndex) {
				return pastType.types.map((type) => {
					return {
						name: type.type.name,
						icon: `/icons/types/${type.type.name}.webp`
					};
				});
			}
		})
		.flat()
		.filter((a) => a !== undefined) as {
		name: string;
		icon: string;
	}[];

	return pastTypes.length > 0 ? pastTypes : vanillaTypes;
};
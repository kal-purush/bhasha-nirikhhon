export interface IPokeApiConfig {
    baseUrl?: string;
}

export interface IPokemonPaginateResponse {
    next: string;
    count: number;
    results: Array<IPokemonBasicResponse>;
    previous: string;
}

export interface IPokemonBasicResponse {
    url: string;
    name: string;
}

export interface IPokemonByNameResponse {
    order: number;
    types: Array<IPokemonTypeResponse>;
    moves: Array<IPokemonMoveResponse>;
    stats: Array<IPokemonStatResponse>;
    sprites: IPokemonSpritesResponse;
    abilities: Array<IPokemonAbilityResponse>;
}

interface IPokemonTypeResponse {
    slot: number;
    type: IPokemonBasicResponse;
}

interface IPokemonMoveResponse {
    move: IPokemonBasicResponse;
}

interface IPokemonStatResponse {
    stat: IPokemonBasicResponse;
    base_stat: number;
}

interface IPokemonSpritesResponse {
    other: {
        dream_world: {
            back_gray?: string;
            front_gray?: string;
            back_shiny?: string;
            front_shiny?: string;
            back_female?: string;
            front_female?: string;
            back_default?: string;
            front_default?: string;
            back_transparent?: string;
            front_transparent?: string;
            back_shiny_female?: string;
            front_shiny_female?: string;
            back_shiny_transparent?: string;
            front_shiny_transparent?: string;
        };
    };
    front_default: string;
}

interface IPokemonAbilityResponse {
    slot: number;
    ability: IPokemonBasicResponse;
    is_hidden: boolean;
}

export interface ISpecieByPokemonNameResponse {
    shape: IPokemonBasicResponse;
    habitat: IPokemonBasicResponse;
    is_baby: boolean;
    gender_rate: number;
    is_mythical: boolean;
    capture_rate: number;
    is_legendary: boolean;
    hatch_counter: number;
    base_happiness: number;
    evolution_chain: Pick<IPokemonBasicResponse, 'url'>;
    evolves_from_species?: IPokemonBasicResponse;
    has_gender_differences: boolean;
}

export interface IEvolutionByOrderResponse {
    chain: {
        species: IPokemonBasicResponse;
        evolves_to: Array<IEvolvesTo>;
    }
}

interface IEvolvesTo {
    species: IPokemonBasicResponse;
    evolves_to: Array<IEvolvesTo>;
}

export interface IMoveByOrderResponse {
    pp: number;
    type: IPokemonBasicResponse;
    name: string;
    power: number;
    target: IPokemonBasicResponse;
    priority: number;
    accuracy: number;
    effect_entries: Array<{
        effect: string;
        short_effect: string;
    }>;
    damage_class: IPokemonBasicResponse;
    effect_chance?: number;
    learned_by_pokemon: Array<IPokemonBasicResponse>;
}
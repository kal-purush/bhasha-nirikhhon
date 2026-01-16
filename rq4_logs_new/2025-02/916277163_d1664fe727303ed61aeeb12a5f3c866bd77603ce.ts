export interface IPokemonExternalPaginate {
    next: string;
    count: number;
    results: Array<IPokemonExternalBasicResponse>;
    previous: string;
}

export interface IPokemonExternalBasicResponse {
    url: string;
    name: string;
}

export interface IExternalResponseOfEvolutionByUrl {
    chain: {
        species: IPokemonExternalBasicResponse;
        evolves_to: Array<IEvolvesTo>;
    };
}

interface IEvolvesTo {
    species: IPokemonExternalBasicResponse;
    evolves_to: Array<IEvolvesTo>;
}

export type IPokemonMovesInformationExternalResponse =
    Array<IPokemonMoveInformationExternalResponse>;

export interface IPokemonMoveInformationExternalResponse {
    move: IPokemonExternalBasicResponse;
}

export interface IExternalResponseOfMoveByUrl {
    pp: number;
    type: IPokemonExternalBasicResponse;
    name: string;
    power: number;
    target: IPokemonExternalBasicResponse;
    priority: number;
    accuracy: number;
    effect_entries: Array<{
        effect: string;
        short_effect: string;
    }>;
    damage_class: IPokemonExternalBasicResponse;
    effect_chance?: number;
    learned_by_pokemon: Array<IPokemonExternalBasicResponse>;
}

export interface IExternalResponseOfPokemonAttributesByPokemonName {
    order: number;
    types: IPokemonTypesInformationExternalResponse;
    moves: IPokemonMovesInformationExternalResponse;
    stats: IPokemonStatsInformationExternalResponse;
    sprites: IPokemonSpritesInformationExternalResponse;
    abilities: IPokemonAbilitiesInformationExternalResponse;
}

export type IPokemonTypesInformationExternalResponse =
    Array<IPokemonTypeInformationExternalResponse>;

export interface IPokemonTypeInformationExternalResponse {
    slot: number;
    type: IPokemonExternalBasicResponse;
}

export type IPokemonStatsInformationExternalResponse =
    Array<IPokemonStatInformationExternalResponse>;

export interface IPokemonStatInformationExternalResponse {
    stat: IPokemonExternalBasicResponse;
    base_stat: number;
}

export interface IPokemonSpritesInformationExternalResponse {
    other: {
        dream_world: IPokemonSpritesUrlInformationExternalResponse;
    };
    front_default: string;
}

interface IPokemonSpritesUrlInformationExternalResponse {
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
}

export type IPokemonAbilitiesInformationExternalResponse =
    Array<IPokemonAbilityInformationExternalResponse>;

export interface IPokemonAbilityInformationExternalResponse {
    slot: number;
    ability: IPokemonExternalBasicResponse;
    is_hidden: boolean;
}

export interface IExternalResponseOfPokemonSpecieByPokemonName {
    shape: IPokemonExternalBasicResponse;
    habitat: IPokemonExternalBasicResponse;
    is_baby: boolean;
    gender_rate: number;
    is_mythical: boolean;
    capture_rate: number;
    is_legendary: boolean;
    hatch_counter: number;
    base_happiness: number;
    evolution_chain: Pick<IPokemonExternalBasicResponse, 'url'>;
    evolves_from_species?: IPokemonExternalBasicResponse;
    has_gender_differences: boolean;
}
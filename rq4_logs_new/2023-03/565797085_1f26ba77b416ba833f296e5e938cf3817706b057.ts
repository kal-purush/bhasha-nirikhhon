export enum InputType {
  SELECT = 'select',
  TEXT = 'text',
  NUMBER = 'number'
}

export interface EpisodeFilters {
  values: {
    name: string
    episode: string
    character_name: string
    take: string
  }
  type: {
    name: InputType
    episode: InputType
    character_name: InputType
    take: InputType
  }
}

export interface LocationFilters {
  values: {
    name: string
    dimension: string
    resident_name: string
    type: string
    take: string
  }
  type: {
    name: InputType
    dimension: InputType
    resident_name: InputType
    type: InputType
    take: InputType
  }
}

export interface CharacterFilters {
  values: {
    name: string
    status: string
    gender: string
    species: string
    take: string
  }
  type: {
    name: InputType
    status: InputType
    gender: InputType
    species: InputType
    take: InputType
  }
}

export type Filters = CharacterFilters | LocationFilters | EpisodeFilters
export type FiltersValues = Filters['values']
export type FiltersTypes = Filters['type']
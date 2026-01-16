export type AboutMe = {
  readonly BIRTHDAY: AboutMeFact
  readonly AGE: AboutMeFact
  readonly ZODIAC: AboutMeFact
  readonly EDUCATION: {
    [key: string]: AboutMeEvent
  }
  readonly CAREER: readonly Job[]
  readonly GRANTS: AboutMeEvent
  readonly LANGUAGES: readonly Language[]
  readonly PERSONAL: readonly AboutMeFact[]
  readonly HOBBIES: readonly AboutMeFact[]
}

export interface AboutMeFact {
  readonly value?: string | number
  readonly translationKey?: string
  readonly extra?: string
  readonly link?: string
  readonly emoji?: string
}

export interface AboutMeEvent extends AboutMeFact {
  readonly start: string
  readonly finish?: string
  readonly locationKey?: string
  readonly countryKey?: string
  readonly link?: string
}

export interface Job extends AboutMeEvent {
  readonly company: string
}

type Language = {
  readonly translationKey: string
  readonly level?: string
  readonly levelKey: string
  readonly emoji?: string
}
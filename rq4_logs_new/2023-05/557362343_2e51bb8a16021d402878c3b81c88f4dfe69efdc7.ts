export enum Region {
  Africa = 'Africa',
  Americas = 'Americas',
  Asia = 'Asia',
  Europe = 'Europe',
  Oceania = 'Oceania',
}

export interface Country {
  name: Name;
  cca3: string;
  status: Status;
  idd: Idd;
  capital: string[];
  altSpellings: string[];
  region: string;
  subregion: Subregion;
  languages: Languages;
  translations: { [key: string]: Translation };
  latlng: number[];
  landlocked: boolean;
  borders?: string[];
  area: number;
  demonyms: Demonyms;
  flag: string;
  maps: Maps;
  population: number;
  gini?: { [key: string]: number };
  fifa?: string;
  car: Car;
  timezones: string[];
  continents: string;
  flags: Flags;
  coatOfArms: CoatOfArms;
  startOfWeek: StartOfWeek;
  capitalInfo: CapitalInfo;
  postalCode?: PostalCode;
}

export interface CapitalInfo {
  latlng: number[];
}

export interface Car {
  signs: string[];
  side: Side;
}

export enum Side {
  Left = 'left',
  Right = 'right',
}

export interface CoatOfArms {
  png?: string;
  svg?: string;
}

export interface Currencies {
  CHF?: All;
  EUR?: All;
  SEK?: All;
  NOK?: All;
  GBP?: All;
  IMP?: All;
  MDL?: All;
  ALL?: All;
  BAM?: BAM;
  RUB?: All;
  GGP?: All;
  BYN?: All;
  BGN?: All;
  DKK?: All;
  FOK?: All;
  RSD?: All;
  JEP?: All;
  RON?: All;
  GIP?: All;
  MKD?: All;
  PLN?: All;
  CZK?: All;
  ISK?: All;
  HUF?: All;
  UAH?: All;
}

export interface All {
  name: string;
  symbol: string;
}

export interface BAM {
  name: string;
}

export interface Demonyms {
  eng: Eng;
  fra?: Eng;
}

export interface Eng {
  f: string;
  m: string;
}

export interface Flags {
  png: string;
  svg: string;
  alt?: string;
}

export interface Idd {
  root: string;
  suffixes: string[];
}

export interface Languages {
  fra?: string;
  gsw?: string;
  ita?: string;
  roh?: string;
  fin?: string;
  swe?: string;
  eng?: string;
  mlt?: string;
  nno?: string;
  nob?: string;
  smi?: string;
  slv?: string;
  ell?: string;
  glv?: string;
  sqi?: string;
  srp?: string;
  ron?: string;
  bos?: string;
  hrv?: string;
  por?: string;
  rus?: string;
  slk?: string;
  nfr?: string;
  lit?: string;
  bel?: string;
  bul?: string;
  dan?: string;
  fao?: string;
  lat?: string;
  est?: string;
  deu?: string;
  de?: string;
  nrf?: string;
  spa?: string;
  nor?: string;
  ltz?: string;
  nld?: string;
  cat?: string;
  tur?: string;
  mkd?: string;
  pol?: string;
  lav?: string;
  gle?: string;
  ces?: string;
  isl?: string;
  cnr?: string;
  hun?: string;
  ukr?: string;
}

export interface Maps {
  googleMaps: string;
  openStreetMaps: string;
}

export interface Name {
  common: string;
  official: string;
  nativeName: { [key: string]: Translation };
}

export interface Translation {
  official: string;
  common: string;
}

export interface PostalCode {
  format: string;
  regex: string;
}

export enum StartOfWeek {
  Monday = 'monday',
}

export enum Status {
  OfficiallyAssigned = 'officially-assigned',
  UserAssigned = 'user-assigned',
}

export enum Subregion {
  CentralEurope = 'Central Europe',
  EasternEurope = 'Eastern Europe',
  NorthernEurope = 'Northern Europe',
  SoutheastEurope = 'Southeast Europe',
  SouthernEurope = 'Southern Europe',
  WesternEurope = 'Western Europe',
}
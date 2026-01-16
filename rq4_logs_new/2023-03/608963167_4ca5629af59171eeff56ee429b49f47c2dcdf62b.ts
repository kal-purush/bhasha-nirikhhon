export interface MangaObject {
  result: string;
  response: string;
  data: MangaData;
  limit: number;
  offset: number;
  total: number;
}

export interface MangaData {
  id: string;
  type: string;
  attributes: Attributes2;
  relationships: Relationship[];
}

interface Relationship {
  id: string;
  type: string;
  related?: string;
}

interface Attributes2 {
  title: Title;
  altTitles: (
    | AltTitle
    | AltTitles2
    | Title
    | AltTitles4
    | AltTitles5
    | AltTitles6
    | AltTitles7
    | AltTitles8
    | AltTitles9
    | AltTitles10
    | AltTitles11
    | AltTitles12
    | AltTitles13
    | AltTitles14
  )[];
  description: Description;
  isLocked: boolean;
  links: Links;
  originalLanguage: string;
  lastVolume?: string;
  lastChapter?: string;
  publicationDemographic?: string;
  status: string;
  year?: number;
  contentRating: string;
  tags: Tag[];
  state: string;
  chapterNumbersResetOnNewVolume: boolean;
  createdAt: string;
  updatedAt: string;
  version: number;
  availableTranslatedLanguages: string[];
  latestUploadedChapter: string;
}

interface Tag {
  id: string;
  type: string;
  attributes: Attributes;
  relationships: any[];
}

interface Attributes {
  name: Title;
  description: Description2;
  group: string;
  version: number;
}

interface Description2 {}

interface Links {
  al?: string;
  mu?: string;
  amz?: string;
  mal?: string;
  raw?: string;
  engtl?: string;
  ap?: string;
  bw?: string;
  kt?: string;
  nu?: string;
  cdj?: string;
  ebj?: string;
}

interface Description {
  en?: string;
  "pt-br"?: string;
  pl?: string;
  ja?: string;
  vi?: string;
  "es-la"?: string;
}

interface AltTitles14 {
  ja: string;
}

interface AltTitles13 {
  "ja-ro"?: string;
  ja?: string;
  en?: string;
}

interface AltTitles12 {
  "pt-br": string;
}

interface AltTitles11 {
  th?: string;
  ja?: string;
  zh?: string;
}

interface AltTitles10 {
  en?: string;
  ja?: string;
  "pt-br"?: string;
  vi?: string;
}

interface AltTitles9 {
  "zh-hk"?: string;
  "ja-ro"?: string;
  ja?: string;
  ne?: string;
  en?: string;
  uk?: string;
}

interface AltTitles8 {
  en?: string;
  ja?: string;
}

interface AltTitles7 {
  ja?: string;
  zh?: string;
  "zh-hk"?: string;
  vi?: string;
  ko?: string;
  "es-la"?: string;
  es?: string;
  en?: string;
  "pt-br"?: string;
}

interface AltTitles6 {
  "ja-ro"?: string;
  ja?: string;
  "zh-hk"?: string;
}

interface AltTitles5 {
  zh?: string;
  "zh-hk"?: string;
  ja?: string;
  en?: string;
}

interface AltTitles4 {
  zh: string;
}

interface AltTitles2 {
  en?: string;
  "ja-ro"?: string;
  fr?: string;
  ru?: string;
  ja?: string;
  ko?: string;
  "zh-hk"?: string;
}

interface AltTitle {
  en?: string;
  "zh-ro"?: string;
  ru?: string;
  zh?: string;
}

interface Title {
  en: string;
}
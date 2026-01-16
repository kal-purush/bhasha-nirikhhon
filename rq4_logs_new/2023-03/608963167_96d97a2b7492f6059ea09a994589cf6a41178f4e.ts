export interface IChpaterFeedObject {
  result: string;
  response: string;
  data: IChapterData[];
  limit: number;
  offset: number;
  total: number;
}

export interface IChapterData {
  id: string;
  type: string;
  attributes: Attributes;
  relationships: Relationship[];
}

interface Relationship {
  id: string;
  type: string;
}

interface Attributes {
  volume?: any;
  chapter: string;
  title?: string;
  translatedLanguage: string;
  externalUrl?: any;
  publishAt: string;
  readableAt: string;
  createdAt: string;
  updatedAt: string;
  pages: number;
  version: number;
}
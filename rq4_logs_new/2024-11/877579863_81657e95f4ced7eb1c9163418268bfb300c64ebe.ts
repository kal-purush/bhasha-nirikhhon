import { Keyword } from './cover-models';

export interface Project {
  name: string;
  info: string;
  description: string;
  url: string;
  order: number;
  image: string;
  keywords: Keyword[];
}
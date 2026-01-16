import { UpdatedAtDocument, CreatedAtDocument } from "../mongodb";

export type SearchHierarchyParams = {
  category: string;
  keyword?: string;
  superior?: string;
  withPayload?: boolean;
};

export enum EnumHierarchyType {
  GROUP = 'GROUP',
  ITEM = 'ITEM',
}

export type HierarchyType = keyof typeof EnumHierarchyType;

export type Hierarchy<P = never> = {
  title: string;
  type: EnumHierarchyType;
  category: string;
  description?: string;
  superior?: string;
  payload?: P;
} & UpdatedAtDocument & CreatedAtDocument;

export type HierarchyData<P = never> = {
  id: string;
} & Hierarchy<P>;
import { atom } from 'jotai';

export interface SidebarDocument {
  id: string;
  title: string;
}

export const sidebarDocumentsAtom = atom<SidebarDocument[] | undefined>(undefined);
export const sidebarDocumentsCountAtom = atom((get) => (get(sidebarDocumentsAtom) ?? []).length);
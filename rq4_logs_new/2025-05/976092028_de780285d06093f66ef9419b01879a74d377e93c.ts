import { atom } from 'jotai';

export type DocumentSaveStatus = 'initial' | 'inProgress' | 'success' | 'failed';

export interface DocumentType {
  content?: string;
  saveStatus: DocumentSaveStatus;
  lastSavedContent?: string;
}

const initialDocumentState: DocumentType = {
  content: '',
  saveStatus: 'initial',
  lastSavedContent: undefined,
};

export const documentAtom = atom<DocumentType>(initialDocumentState);

export const contentAtom = atom(
  (get) => get(documentAtom).content,
  (get, set, newContent?: string) =>
    set(documentAtom, (prev) => ({ ...prev, content: newContent })),
);

export const saveStatusAtom = atom(
  (get) => get(documentAtom).saveStatus,
  (get, set, newSaveStatus: DocumentSaveStatus) =>
    set(documentAtom, (prev) => ({ ...prev, saveStatus: newSaveStatus })),
);

export const lastSavedContentAtom = atom(
  (get) => get(documentAtom).lastSavedContent,
  (get, set, newLastSavedDocument?: string) =>
    set(documentAtom, (prev) => ({ ...prev, lastSavedContent: newLastSavedDocument })),
);
import { atom } from 'jotai';

export const infoWindowAtom = atom<kakao.maps.InfoWindow | null>(null);
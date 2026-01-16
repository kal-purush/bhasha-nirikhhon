import { atom, selectorFamily, selector } from 'recoil';
import { recoilPersist } from 'recoil-persist';

const { persistAtom } = recoilPersist();

// TODO: any 수정
export const categoryListAtom = atom<any>({
    key: 'categoryList',
    default: [],
});

export const interviewListAtom = atom<any>({
    key: 'interviewList',
    default: null,
    effects_UNSTABLE: [persistAtom],
});

export const interviewIdAtom = atom<any>({
    key: 'interviewId',
    default: null,
    effects_UNSTABLE: [persistAtom],
});
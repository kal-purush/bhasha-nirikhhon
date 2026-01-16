import {atom} from 'recoil';
import {CreateCreditCardDto} from '^types/credit-cards.type';

export const addCardModal = {
    isShowAtom: atom({
        key: 'v3/addCardModal',
        default: false,
    }),
    popStateSyncKey: 'addCardModal',
};

export const inputCardNumberModal = {
    isShowAtom: atom({
        key: 'v3/inputCardNumberModal',
        default: false,
    }),
    popStateSyncKey: 'inputCardNumberModal',
};

export const selectCardCompanyModal = {
    isShowAtom: atom({
        key: 'v3/selectCardCompanyModal',
        default: false,
    }),
    popStateSyncKey: 'selectCardCompanyModal',
};

export const inputCardNameModal = {
    isShowAtom: atom({
        key: 'v3/inputCardNameModal',
        default: false,
    }),
    popStateSyncKey: 'inputCardNameModal',
};

export const inputCardHoldingMemeberModal = {
    isShowAtom: atom({
        key: 'v3/inputCardHoldingMemeberModal',
        default: false,
    }),
    popStateSyncKey: 'inputCardHoldingMemeberModal',
};

export const selectAppModal = {
    isShowAtom: atom({
        key: 'v3/selectAppModal',
        default: false,
    }),
    popStateSyncKey: 'selectAppModal',
};

export const creditcardAtom = atom<CreateCreditCardDto>({
    key: 'creditcardAtom',
    default: {} as CreateCreditCardDto,
});
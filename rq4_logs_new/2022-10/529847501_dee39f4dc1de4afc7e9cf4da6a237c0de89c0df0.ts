import { atom, selector } from 'recoil';

type ExchangeState = {
  upbitBit: number;
  binanceBit: number;
  usdToKrw: number;
  isLoading: boolean;
};

export const exchangeState = atom<ExchangeState>({
  key: 'exchangeState',
  default: {
    upbitBit: 0,
    binanceBit: 0,
    usdToKrw: 0,
    isLoading: true,
  },
});

export const exchangeSelector = selector({
  key: 'exchangeSelector',
  get: ({ get }) => {
    const { upbitBit, binanceBit, usdToKrw, isLoading } = get(exchangeState);
    const convertedToKrw = binanceBit * usdToKrw;
    const bitDiff = ((upbitBit - convertedToKrw) / convertedToKrw) * 100;
    const usdtToKrw = usdToKrw * (1 + bitDiff / 100) ?? 0;

    return {
      usdToKrw,
      upbitBit,
      binanceBit: convertedToKrw,
      usdtToKrw: isNaN(usdtToKrw) ? 0 : usdtToKrw,
      bitDiff: isNaN(bitDiff) || !bitDiff ? 0 : bitDiff,
      isLoading,
    };
  },
});
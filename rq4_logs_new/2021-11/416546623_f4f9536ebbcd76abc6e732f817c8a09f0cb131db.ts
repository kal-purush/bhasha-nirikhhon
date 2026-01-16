import qs from 'qs';
import binance from '../../assets/binance.svg';
import bitfinex from '../../assets/bitfinex.svg';
import kucoin from '../../assets/kucoin.svg';
import transak from '../../assets/transak.svg';
import huobi from '../../assets/huobi.svg';
import okex from '../../assets/okex.svg';
import { Currency } from '../buttons/Buy';

export interface Resource {
  name: string;
  image: string;
  link: string;
}

type Resources = Record<string, Resource[]>;

export const exchanges: Resources = {
  Luna: [
    {
      name: 'Bitfinex',
      image: bitfinex,
      link: 'https://trading.bitfinex.com/t/LUNA:USD',
    },
    {
      name: 'Okex',
      image: okex,
      link: 'https://www.okex.com/trade-spot/luna-usdt',
    },
    {
      name: 'KuCoin',
      image: kucoin,
      link: 'https://trade.kucoin.com/LUNA-USDT',
    },
    {
      name: 'Huobi',
      image: huobi,
      link: 'https://www.huobi.com/en-us/exchange/luna_usdt/',
    },
    {
      name: 'Binance',
      image: binance,
      link: 'https://www.binance.com/en/trade/LUNA_USDT',
    },
  ],
  UST: [
    {
      name: 'Bitfinex',
      image: bitfinex,
      link: 'https://trading.bitfinex.com/t/TERRAUST:USD',
    },
    {
      name: 'KuCoin',
      image: kucoin,
      link: 'https://trade.kucoin.com/USDT-UST',
    },
  ],
};

export const getFiatResources = (
  currency: Currency,
  transakApiKey?: string,
): Resource[] => {
  const link = `https://global.transak.com/?${qs.stringify(
    {
      apiKey: transakApiKey || null,
      cryptoCurrencyList: 'UST,LUNA',
      defaultCryptoCurrency: currency.toUpperCase(),
      networks: 'terra',
    },
    {
      skipNulls: true,
      encode: false,
    },
  )}`;

  return [
    {
      name: 'Transak',
      image: transak,
      link,
    },
  ];
};
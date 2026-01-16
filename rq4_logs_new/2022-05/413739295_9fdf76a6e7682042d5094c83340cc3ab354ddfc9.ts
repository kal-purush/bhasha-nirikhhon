import { binanceAPI } from 'utils/binanceAPIPromise';

export type GetAPIRestrictionsResponse = {
  ipRestrict: boolean;
  createTime: number;
  enableMargin: boolean;
  enableFutures: boolean;
  enableVanillaOptions: boolean;
  enableReading: boolean;
  enableSpotAndMarginTrading: boolean;
  enableWithdrawals: boolean;
  enableInternalTransfer: boolean;
  permitsUniversalTransfer: boolean;
};

export const verifyAPIRestrictions = (apiKey: string, apiSecret: string) =>
  binanceAPI.get({
    url: '/sapi/v1/account/apiRestrictions',
    apiKey,
    apiSecret,
    headers: { 'Content-Type': 'application/json' },
  });
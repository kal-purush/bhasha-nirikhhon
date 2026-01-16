import { useEffect, useMemo } from 'react';
import { useRequest } from 'ahooks';
import axios from 'axios';
import { BEARCHAIN_API } from '@/hooks/use-bgt';
import Big from 'big.js';
import { getTokenLogo } from '@/sections/dashboard/utils';
import { berachain } from '@reown/appkit/networks';

export function useIncentive(props: any) {
  const { vaults = [] } = props;

  const tokens = useMemo(() => {
    const _tokens: string[] = [];
    vaults.forEach((item: any) => {
      item.receivingVault?.activeIncentives?.forEach((incentive: any) => {
        if (_tokens.indexOf(incentive.token.address) === -1) {
          _tokens.push(incentive.token.address);
        }
      })
    });
    return _tokens;
  }, [vaults]);

  const { data: priceData, loading: priceDataLoading, runAsync: getPriceData } = useRequest(async () => {
    const res = await axios.post(BEARCHAIN_API, {
      operationName: "GetTokenCurrentPrices",
      variables :{"chains":["BERACHAIN"],"addressIn": tokens},
      query :"query GetTokenCurrentPrices($chains: [GqlChain!]!, $addressIn: [String!]!) {\n  tokenGetCurrentPrices(chains: $chains, addressIn: $addressIn) {\n    address\n    chain\n    price\n    updatedAt\n    updatedBy\n    __typename\n  }\n}"
    });
    if (res.status !== 200 || !res.data?.data?.tokenGetCurrentPrices) {
      return [];
    }
    return res.data.data.tokenGetCurrentPrices;
  }, {
    manual: true,
  });

  const [list, estReturnPerBGT] = useMemo(() => {
    const totalPercentageNumerator = vaults.reduce((prev: any, curr: any) => Big(prev).plus(curr.percentageNumerator || 0), Big(0));
    // [token.address]: {
    //   address: token.address,
    //   name: token.name,
    //   symbol: token.symbol,
    //   decimals: token.decimals,
    //   vaults: [
    //     Vault
    //   ],
    // }
    const _list = new Map();
    vaults.forEach((item: any, index: number) => {
      item.percentageNumeratorFormatted = Big(item.percentageNumerator || 0).div(totalPercentageNumerator);

      // grouped by incentive.token
      if (item.receivingVault?.activeIncentives?.length > 0) {
        item.receivingVault.activeIncentives.forEach((incentive: any) => {
          incentive.ratePerBGTEmitted = Big(incentive.incentiveRate || 0).times(item.percentageNumeratorFormatted);

          if (_list.has(incentive.token.address)) {
            const _curr = _list.get(incentive.token.address);
            _list.set(incentive.token.address, {
              ..._curr,
              remainingAmount: Big(_curr.remainingAmount).plus(incentive.remainingAmount || 0),
              remainingAmountUsd: Big(_curr.remainingAmountUsd).plus(incentive.remainingAmountUsd || 0),
              ratePerBGTEmitted: Big(_curr.ratePerBGTEmitted || 0).plus(incentive.ratePerBGTEmitted || 0),
              vaults: [..._list.get(incentive.token.address).vaults, item],
            });
          } else {
            const currPrice = priceData?.find((item: any) => item.address === incentive.token.address);
            _list.set(incentive.token.address, {
              address: incentive.token.address,
              name: incentive.token.name,
              symbol: incentive.token.symbol,
              decimals: incentive.token.decimals,
              remainingAmount: Big(incentive.remainingAmount || 0),
              remainingAmountUsd: Big(incentive.remainingAmountUsd || 0),
              ratePerBGTEmitted: Big(incentive.ratePerBGTEmitted || 0),
              price: currPrice?.price || 0,
              icon: getTokenLogo(incentive.token.symbol),
              link: `${berachain.blockExplorers.default.url}/token/${incentive.token.address}`,
              vaults: [item],
            });
          }
        });
      }
    });

    const _estReturnPerBGT = Array.from(_list.values()).reduce((prev: any, curr: any) => Big(prev).plus(Big(curr.ratePerBGTEmitted || 0).times(curr.price || 0)), Big(0));

    return [_list, _estReturnPerBGT];
  }, [vaults, priceData]);

  useEffect(() => {
    if (!tokens.length) return;
    getPriceData();
  }, [tokens]);

  return {
    list: Array.from(list.values()),
    mapList: list,
    estReturnPerBGT,
    priceData,
    priceDataLoading,
  };
}
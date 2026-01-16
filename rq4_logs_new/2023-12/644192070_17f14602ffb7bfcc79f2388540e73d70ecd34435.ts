import { roundToLotString } from '@d8x/perpetuals-sdk';
import { useMemo } from 'react';

import { formatToCurrency } from 'utils/formatToCurrency';
import { PerpetualStaticInfoI } from 'types/types';

export function useMinPositionString(currencyMultiplier: number, perpetualStaticInfo: PerpetualStaticInfoI | null) {
  const minPositionString = useMemo(() => {
    if (perpetualStaticInfo) {
      return formatToCurrency(
        +roundToLotString(10 * perpetualStaticInfo.lotSizeBC, perpetualStaticInfo.lotSizeBC) * currencyMultiplier,
        '',
        false,
        undefined,
        true
      );
    }
    return '0.1';
  }, [perpetualStaticInfo, currencyMultiplier]);

  return { minPositionString };
}
import BigNumber from 'bignumber.js'

import {
  DEFAULT_PRECISION,
  FLATMONEY_COLLATERAL_ADDRESS_MAP,
} from 'core-kit/const'
import { usePoolComposition, usePoolTokenPrice } from 'core-kit/hooks/pool'
import {
  useSendTokenInput,
  useTradingPanelPoolConfig,
} from 'core-kit/hooks/state'
import {
  formatToUsd,
  isEqualAddress,
  isFlatMoneyLeveragedAsset,
} from 'core-kit/utils'

export const useLeveragedFlatMoneyWithdrawalChecks = () => {
  const config = useTradingPanelPoolConfig()
  const composition = usePoolComposition(config)
  const [sendToken] = useSendTokenInput()
  const vaultTokenPrice = usePoolTokenPrice(config)

  const hasLeveragedRethPosition = composition.some(
    ({ tokenAddress, amount }) =>
      isFlatMoneyLeveragedAsset(tokenAddress) && amount !== '0',
  )
  const collateral = composition.find(({ tokenAddress }) =>
    isEqualAddress(
      FLATMONEY_COLLATERAL_ADDRESS_MAP[config.chainId],
      tokenAddress,
    ),
  )

  if (!hasLeveragedRethPosition || !collateral) {
    return {
      requiresLeveragedCollateralLiquidity: false,
      leveragedCollateralValueFormatted: '$0',
    }
  }

  const sendTokenValue =
    Number(sendToken.value || '0') * Number(vaultTokenPrice)
  const collateralValue = new BigNumber(collateral.amount)
    .shiftedBy(-collateral.precision)
    .times(collateral.rate)
    .shiftedBy(-DEFAULT_PRECISION)
    .toNumber()

  return {
    requiresLeveragedCollateralLiquidity: sendTokenValue > collateralValue,
    leveragedCollateralValueFormatted: formatToUsd({ value: collateralValue }),
  }
}
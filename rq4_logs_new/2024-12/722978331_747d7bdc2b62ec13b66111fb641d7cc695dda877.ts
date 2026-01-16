import { useCallback } from 'react'

import { usePublicClient } from 'wagmi'

import { AaveAssetGuardAbi } from 'core-kit/abi'
import { AddressZero } from 'core-kit/const'
import { usePoolStatic } from 'core-kit/hooks/pool/multicall'
import { makeStaticCall } from 'core-kit/hooks/web3/use-static-call-query'
import type {
  CalculateSwapDataParamsResponse,
  PoolConfig,
} from 'core-kit/types'
import { getSlippageToleranceForContractTransaction } from 'core-kit/utils'

export interface FetchAaveSwapParamsProps {
  withdrawAmount: bigint
  slippage: number
}

export const useFetchAaveSwapParams = ({
  address,
  chainId,
}: Pick<PoolConfig, 'address' | 'chainId'>) => {
  const publicClient = usePublicClient({ chainId })

  const { data: { aaveAssetGuardAddress = AddressZero } = {} } = usePoolStatic({
    address,
    chainId,
  })

  return useCallback(
    async ({
      withdrawAmount,
      slippage,
    }: FetchAaveSwapParamsProps): Promise<
      CalculateSwapDataParamsResponse | undefined
    > =>
      makeStaticCall<CalculateSwapDataParamsResponse>({
        address: aaveAssetGuardAddress,
        abi: AaveAssetGuardAbi,
        functionName: 'calculateSwapDataParams',
        args: [
          address,
          withdrawAmount,
          getSlippageToleranceForContractTransaction(slippage),
        ],
        publicClient,
      }),
    [aaveAssetGuardAddress, address, publicClient],
  )
}
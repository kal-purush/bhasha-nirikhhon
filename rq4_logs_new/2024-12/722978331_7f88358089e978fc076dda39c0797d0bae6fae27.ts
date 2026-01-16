import { EasySwapperV2Abi } from 'core-kit/abi'
import { useReadContract } from 'core-kit/hooks/web3'
import type { PoolContractAccountCallParams } from 'core-kit/types'
import { getContractAddressById, isZeroAddress } from 'core-kit/utils'

export const useEasySwapperTrackedAssets = ({
  account,
  chainId,
}: Omit<PoolContractAccountCallParams, 'address'>) =>
  useReadContract({
    address: getContractAddressById('easySwapperV2', chainId),
    abi: EasySwapperV2Abi,
    functionName: 'getTrackedAssets',
    chainId,
    args: [account],
    query: { enabled: !isZeroAddress(account) },
  })
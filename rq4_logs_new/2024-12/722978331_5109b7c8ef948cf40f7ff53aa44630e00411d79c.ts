import { PoolFactoryAbi } from 'core-kit/abi'
import { usePoolComposition } from 'core-kit/hooks/pool/use-pool-composition'
import { useReadContracts } from 'core-kit/hooks/web3'
import type { PoolContractCallParams } from 'core-kit/types'
import { getContractAddressById } from 'core-kit/utils'

export const useHasDhedgeVaultInComposition = ({
  address,
  chainId,
}: PoolContractCallParams) => {
  const composition = usePoolComposition({
    address,
    chainId,
  })

  return useReadContracts({
    contracts: composition.map(({ tokenAddress }) => ({
      address: getContractAddressById('factory', chainId),
      abi: PoolFactoryAbi,
      functionName: 'isPool',
      chainId,
      args: [tokenAddress],
    })),
    query: {
      select: (data) =>
        composition.some(
          ({ amount }, index) => !!data[index]?.result && amount !== '0',
        ),
      staleTime: Infinity,
    },
  })
}
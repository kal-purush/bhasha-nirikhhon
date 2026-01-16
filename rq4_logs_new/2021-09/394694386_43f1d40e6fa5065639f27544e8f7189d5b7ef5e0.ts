import { Token } from '@ubeswap/sdk'
import { BridgeableTokens, OpticsDevDomains, OpticsDomainInfo, OpticsMainnetDomains } from 'constants/Optics'
import { useActiveWeb3React, useWeb3ChainId } from 'hooks'

export const useNetworkDomains = (): OpticsDomainInfo[] => {
  const { chainId } = useActiveWeb3React()
  if (OpticsMainnetDomains.findIndex((domain) => domain.chainId === chainId) > -1) {
    return OpticsMainnetDomains
  }
  return OpticsDevDomains
}

export const useBridgeableTokens = (): { [address: string]: Token } => {
  const chainId = useWeb3ChainId()
  const tokenList: Token[] = BridgeableTokens[chainId] as any as Token[]
  const tokenMap: { [address: string]: Token } = {}
  tokenList.forEach((token) => (tokenMap[token.address] = token))
  console.log({
    tokenList,
    tokenMap,
  })
  return tokenMap
}
import { useTradingPanelPoolConfig } from 'core-kit/hooks/state'
import { useIsDhedgeVaultConnected } from 'core-kit/hooks/user'
import { useConfigContextParams } from 'trading-widget/providers/config-provider'

export const useIsOffchainAaveWithdrawSupported = () => {
  const { chainId } = useTradingPanelPoolConfig()
  const { aaveOffchainWithdrawChainIds } = useConfigContextParams()
  const isDhedgeVaultConnected = useIsDhedgeVaultConnected()

  return (
    aaveOffchainWithdrawChainIds.includes(chainId) && !isDhedgeVaultConnected
  )
}
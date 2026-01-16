import { ChainId } from '@ubeswap/sdk'
import { useActiveContractKit } from 'hooks'
import { SwapCallbackState, useSwapCallback } from 'hooks/useMentoCallback'
import { useMemo } from 'react'

import { INITIAL_ALLOWED_SLIPPAGE } from '../../../constants'
import { MentoTrade } from '../../../state/mento/hooks'
import { useDoTransaction } from '.'

/**
 * Use callback to allow trading
 * @param trade
 * @param allowedSlippage
 * @param recipientAddressOrName
 * @returns
 */
export const useMentoTradeCallback = (
  trade: MentoTrade | undefined, // trade to execute, required
  allowedSlippage: number = INITIAL_ALLOWED_SLIPPAGE, // in bips
  recipientAddressOrName: string | null // the ENS name or address of the recipient of the trade, or null if swap should be returned to sender
): { state: SwapCallbackState; callback: null | (() => Promise<string>); error: string | null } => {
  const { library, chainId, account } = useActiveContractKit()
  const doTransaction = useDoTransaction()

  const {
    state: swapState,
    callback: swapCallback,
    error,
  } = useSwapCallback(trade, allowedSlippage, recipientAddressOrName)

  return useMemo(() => {
    if (error) {
      return { state: swapState, callback: null, error }
    }

    if (!library || !trade || !account) {
      return { state: SwapCallbackState.INVALID, callback: null, error: 'Missing dependencies' }
    }

    if (chainId === ChainId.BAKLAVA) {
      return { state: SwapCallbackState.INVALID, callback: null, error: 'Baklava is not supported' }
    }

    const signer = library.getSigner(account)
    const env = { signer, chainId, doTransaction }
    if (swapCallback) {
      return { state: SwapCallbackState.VALID, callback: swapCallback, error: null }
    } else {
      return { state: SwapCallbackState.INVALID, callback: null, error: 'Unknown trade type' }
    }
  }, [swapCallback, library, chainId, doTransaction, trade, account, error, swapState])
}
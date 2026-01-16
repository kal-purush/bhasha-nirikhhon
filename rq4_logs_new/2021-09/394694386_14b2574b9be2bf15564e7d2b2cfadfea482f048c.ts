import { JSBI, TokenAmount } from '@ubeswap/sdk'
import { useMobi, useVeMobi } from 'hooks/Tokens'
import { useSelector } from 'react-redux'
import { AppState } from 'state'
import { StableSwapPool } from 'state/stablePools/reducer'

import { StakingState } from './reducer'

export type GaugeSummary = {
  pool: string
  address: string
  baseBalance: TokenAmount
  boostedBalance: TokenAmount
}

export type MobiStakingInfo = {
  votingPower: TokenAmount
  totalVotingPower: TokenAmount
  mobiLocked?: TokenAmount
  lockEnd?: Date
  positions?: GaugeSummary[]
}

export function calculateBoostedBalance(
  votingPower: JSBI,
  totalVotingPower: JSBI,
  liquidity: JSBI,
  totalLiquidity: JSBI
): JSBI {
  const boosted = JSBI.add(
    JSBI.divide(JSBI.multiply(JSBI.BigInt(4), liquidity), JSBI.BigInt(10)),
    JSBI.divide(
      JSBI.multiply(JSBI.BigInt(6), JSBI.multiply(totalLiquidity, votingPower)),
      JSBI.multiply(totalVotingPower, JSBI.BigInt(10))
    )
  )
  return JSBI.greaterThan(boosted, liquidity) ? boosted : liquidity
}

export function useMobiStakingInfo(): MobiStakingInfo {
  const stakingInfo = useSelector<AppState, StakingState>((state) => state.staking)
  const pools = useSelector<AppState, StableSwapPool[]>((state) => {
    const allPools = state.stablePools.pools
    return Object.values(allPools).map(({ pool }) => pool)
  })
  const veMobi = useVeMobi()
  const mobi = useMobi()
  const baseInfo: MobiStakingInfo = {
    votingPower: new TokenAmount(veMobi, stakingInfo.votingPower),
    totalVotingPower: new TokenAmount(veMobi, stakingInfo.totalVotingPower),
    mobiLocked: new TokenAmount(mobi, stakingInfo.locked?.amount ?? '0'),
    lockEnd: stakingInfo.locked ? new Date(stakingInfo.locked.end) : undefined,
  }
  if (pools && pools.length === 0) {
    return baseInfo
  }
  const positions = pools
    .filter((pool) => pool.gaugeAddress && JSBI.greaterThan(pool.staking?.userStaked ?? JSBI.BigInt(0), JSBI.BigInt(0)))
    .map((pool) => ({
      pool: pool.name,
      address: pool.gaugeAddress ?? '',
      baseBalance: new TokenAmount(pool.lpToken, pool.staking?.userStaked ?? '0'),
      boostedBalance: new TokenAmount(
        pool.lpToken,
        calculateBoostedBalance(
          stakingInfo.votingPower,
          stakingInfo.totalVotingPower,
          pool.staking?.userStaked ?? JSBI.BigInt('0'),
          pool.staking?.totalStakedAmount ?? JSBI.BigInt('1')
        )
      ),
    }))
  return {
    ...baseInfo,
    positions,
  }
}
import { createReducer } from '@reduxjs/toolkit'
import { JSBI } from '@ubeswap/sdk'

import { updateStaking } from './actions'

export type VoteLock = {
  amountOfMOBI: JSBI
  end: number // UNIX time stamp
}
export type StakingState = {
  votingPower: JSBI
  totalVotingPower: JSBI
  locked?: VoteLock
}

const initialState: StakingState = {
  votingPower: JSBI.BigInt(0),
  totalVotingPower: JSBI.BigInt(0),
}

export default createReducer<StakingState>(initialState, (builder) =>
  builder.addCase(updateStaking, (state, { payload: { stakingInfo } }) => ({
    ...state,
    ...stakingInfo,
  }))
)
import { createSelector } from '@reduxjs/toolkit'
import { selectAllChampion } from './champion.slice'
import { selectMasteryEntities } from './mastery.slice'

export const selectAllJoinedChampionMastery = createSelector(
  selectAllChampion,
  selectMasteryEntities,
  (champions, masteryEntities) => {
    champions.map((champion) => {
      return {
        champion,
        mastery: masteryEntities[parseInt(champion.key)],
      }
    })
  }
)
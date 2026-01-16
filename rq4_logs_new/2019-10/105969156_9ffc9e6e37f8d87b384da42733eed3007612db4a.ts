/*
  'https://werewolf.world/village/schema/0.3/votingResult.json'
*/
import {Character} from './character'

export interface VotingResult {
  votingResultsDetails: {
    '@id': string
    sourceCharacter: {
      '@id': Character['@id']
      id: Character['id']
      image: Character['image']
      name: Character['name']
    }
    targetAgent: {
      '@id': Character['@id']
      id: Character['id']
      image: Character['image']
      name: Character['name']
    }
  }[]
  votingResultsSummary: {
    '@id': string
    characterToLynch: {
      '@id': Character['@id']
      id: Character['id']
      image: Character['image']
      name: Character['name']
    }
    numberOfVotes: number
    rankOfVotes: number
  }[]
}
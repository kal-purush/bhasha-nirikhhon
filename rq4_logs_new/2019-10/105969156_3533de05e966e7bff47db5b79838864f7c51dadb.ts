/*
  'https://werewolf.world/village/schema/0.3/character.json'
*/
import {
  AgentStatus,
  Context,
  Gender,
  LanguageMap,
  Payload$AgentId,
  Result,
  Time
} from '..'

export interface Character {
  '@context': Context.Character
  '@id': string
  fullName: LanguageMap
  gender: Gender
  id: Payload$AgentId
  image: string
  isAChoice: boolean
  isMine: boolean
  name: LanguageMap
  result: Result
  status: AgentStatus
  update: {
    '@id': Time['@id']
    date: Time['date']
    phase: Time['phase']
  }
}
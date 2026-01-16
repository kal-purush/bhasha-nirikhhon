/*
  'https://werewolf.world/village/schema/0.3/voteMessage.json'
*/
import {Base} from './base'
import {Character} from './character'

export interface VoteMessage {
  '@context': Base['@context']
  '@id': Base['@id']
  character?: {
    '@context': Character['@context']
    '@id': Character['@id']
    id: Character['id']
    image: Character['image']
    name: Character['name']
  }
  clientTimestamp: Base['clientTimestamp']
  date: Base['date']
  directionality: Base['directionality']
  extensionalDisclosureRange: Base['extensionalDisclosureRange']
  intensionalDisclosureRange: Base['intensionalDisclosureRange']
  phase: Base['phase']
  phaseStartTime: Base['phaseStartTime']
  phaseTimeLimit: Base['phaseTimeLimit']
  serverTimestamp: Base['serverTimestamp']
  token: Base['token']
  village: Base['village']
}
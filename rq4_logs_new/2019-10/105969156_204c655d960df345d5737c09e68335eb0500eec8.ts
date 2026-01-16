/*
  'https://werewolf.world/village/schema/0.3/flavorTextMessage.json'
*/
import {Base} from './base'
import {ChatMessage} from './chatMessage'

export interface FlavorTextMessage {
  '@context': Base['@context']
  '@id': Base['@id']
  clientTimestamp: Base['clientTimestamp']
  date: Base['date']
  directionality: Base['directionality']
  extensionalDisclosureRange: Base['extensionalDisclosureRange']
  flavorText: ChatMessage[]
  intensionalDisclosureRange: Base['intensionalDisclosureRange']
  phase: Base['phase']
  phaseStartTime: Base['phaseStartTime']
  phaseTimeLimit: Base['phaseTimeLimit']
  serverTimestamp: Base['serverTimestamp']
  token: Base['token']
  village: Base['village']
}
/*
  'https://werewolf.world/village/schema/0.3/scrollMessage.json'
*/
import {Base} from './base'
import {Scroll} from './scroll'

export interface ScrollMessage {
  '@context': Base['@context']
  '@id': Base['@id']
  clientTimestamp: Base['clientTimestamp']
  date: Base['date']
  directionality: Base['directionality']
  extensionalDisclosureRange: Base['extensionalDisclosureRange']
  intensionalDisclosureRange: Base['intensionalDisclosureRange']
  nodeId: Scroll['nodeId']
  offsetHeight: Scroll['offsetHeight']
  phase: Base['phase']
  phaseStartTime: Base['phaseStartTime']
  phaseTimeLimit: Base['phaseTimeLimit']
  scrollHeight: Scroll['scrollHeight']
  scrollTop: Scroll['scrollTop']
  serverTimestamp: Base['serverTimestamp']
  token: Base['token']
  village: Base['village']
}
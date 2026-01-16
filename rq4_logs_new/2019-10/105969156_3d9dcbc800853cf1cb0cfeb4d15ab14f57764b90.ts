/*
  'https://werewolf.world/village/schema/0.3/errorMessage.json'
*/
import {Base} from './base'
import {Error} from './error'

export interface ErrorMessage {
  '@context': Base['@context']
  '@id': Base['@id']
  clientTimestamp: Base['clientTimestamp']
  content: Error['content']
  date: Base['date']
  directionality: Base['directionality']
  extensionalDisclosureRange: Base['extensionalDisclosureRange']
  intensionalDisclosureRange: Base['intensionalDisclosureRange']
  isFromServer: Error['isFromServer']
  phase: Base['phase']
  phaseStartTime: Base['phaseStartTime']
  phaseTimeLimit: Base['phaseTimeLimit']
  serverTimestamp: Base['serverTimestamp']
  severity: Error['severity']
  source: Error['source']
  token: Base['token']
  village: Base['village']
}
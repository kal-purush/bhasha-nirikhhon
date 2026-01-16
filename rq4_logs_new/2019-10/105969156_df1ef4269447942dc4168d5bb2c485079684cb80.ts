/*
  'https://werewolf.world/village/schema/0.3/boardMessage.json'
*/
import {Base} from './base'
import {Board} from './board'
import {Character} from './character'
import {Role} from './role'

export interface BoardMessage {
  '@context': Base['@context']
  '@id': Base['@id']
  character: {
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
  prediction: Board['prediction']
  role: {
    '@context': Role['@context']
    '@id': Role['@id']
    image: Role['image']
    name: Role['name']
  }
  serverTimestamp: Base['serverTimestamp']
  token: Base['token']
  village: Base['village']
}
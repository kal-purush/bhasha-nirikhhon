/*
  'https://werewolf.world/village/schema/0.3/base.json'
*/
import {
  BaseContext,
  Channel,
  Directionality,
  PayloadBase
} from '..'
import {Avatar} from './avatar'
import {Character} from './character'
import {Chat} from './chat'
import {ChatSettings} from './chatSettings'
import {Role} from './role'
import {Time} from './time'
import {Village} from './village'

export interface Base extends PayloadBase {
  '@context': BaseContext[]
  '@id': string
  avatar: {
    '@context': Avatar['@context']
    '@id': Avatar['@id']
    token: Avatar['token']
    name: Avatar['name']
    image: Avatar['image']
  }
  clientTimestamp: string
  date: Time['date']
  directionality: Directionality
  extensionalDisclosureRange: {
    '@context': Character['@context']
    '@id': Character['@id']
    id: Character['id']
    image: Character['image']
    isHumanPlayer: boolean
    name: Character['name']
    role: {
      '@context': Role['@context']
      '@id': Role['@id']
      image: Role['image']
      name: Role['name']
    }
    status: Character['status']
  }[]
  intensionalDisclosureRange: Channel
  myCharacter: {
    '@context': Character['@context']
    '@id': Character['@id']
    id: Character['id']
    image: Character['image']
    name: Character['name']
    role: {
      '@context': Role['@context']
      '@id': Role['@id']
      image: Role['image']
      name: Role['name']
    }
  }
  phase: Time['phase']
  phaseStartTime: string
  phaseTimeLimit: number
  serverTimestamp: string
  token: Avatar['token']
  village: {
    '@context': Village['@context']
    '@id': Village['@id']
    chatSettings: {
      '@context': ChatSettings['@context']
      '@id': ChatSettings['@id']
      maxLengthOfUnicodeCodePoints: Chat['maxLengthOfUnicodeCodePoints']
      maxNumberOfChatMessages: Chat['maxNumberOfChatMessages']
    }
    id: Village['id']
    name: Village['name']
    lang: Village['lang']
    totalNumberOfCharacters: Village['totalNumberOfCharacters']
  }
}
import {
  AdvancedSearch,
  Avatar,
  BuildVillage,
  ChangeLang,
  ChangeUserEmail,
  ChangeUserName,
  ChangeUserPassword,
  EnterLobby,
  GetAvatar,
  GetSettings,
  IdSearch,
  KickOutPlayer,
  LeaveWaitingPage,
  Lobby,
  Ping,
  Play,
  Played,
  Pong,
  Ready,
  SearchResult,
  SelectVillage,
  Settings,
  WatingPage
} from '../schema'

export interface PayloadBase {
  type: PayloadType
}
export const enum PayloadType {
  advancedSearch = 'advancedSearch',
  avatar = 'avatar',
  buildVillage = 'buildVillage',
  changeLang = 'changeLang',
  changeUserEmail = 'changeUserEmail',
  changeUserName = 'changeUserName',
  changeUserPassword = 'changeUserPassword',
  enterLobby = 'enterLobby',
  getAvatar = 'getAvatar',
  getSettings = 'getSettings',
  idSearch = 'idSearch',
  kickOutPlayer = 'kickOutPlayer',
  leaveWaitingPage = 'leaveWaitingPage',
  lobby = 'lobby',
  ping = 'ping',
  play = 'play',
  played = 'played',
  pong = 'pong',
  ready = 'ready',
  searchResult = 'searchResult',
  selectVillage = 'selectVillage',
  settings = 'settings',
  waitingPage = 'waitingPage'
}
export type Payload =
  | Payload$AdvancedSearch
  | Payload$Avatar
  | Payload$BuildVillage
  | Payload$ChangeLang
  | Payload$ChangeUserEmail
  | Payload$ChangeUserName
  | Payload$ChangeUserPassword
  | Payload$EnterLobby
  | Payload$GetAvatar
  | Payload$GetSettings
  | Payload$IdSearch
  | Payload$KickOutPlayer
  | Payload$LeaveWaitingPage
  | Payload$Lobby
  | Payload$Ping
  | Payload$Play
  | Payload$Played
  | Payload$Pong
  | Payload$Ready
  | Payload$SearchResult
  | Payload$SelectVillage
  | Payload$Settings
  | Payload$WatingPage

export interface Payload$AdvancedSearch extends AdvancedSearch {}
export interface Payload$Avatar extends Avatar {}
export interface Payload$BuildVillage extends BuildVillage {}
export interface Payload$ChangeLang extends ChangeLang {}
export interface Payload$ChangeUserEmail extends ChangeUserEmail {}
export interface Payload$ChangeUserName extends ChangeUserName {}
export interface Payload$ChangeUserPassword extends ChangeUserPassword {}
export interface Payload$EnterLobby extends EnterLobby {}
export interface Payload$GetAvatar extends GetAvatar {}
export interface Payload$GetSettings extends GetSettings {}
export interface Payload$IdSearch extends IdSearch {}
export interface Payload$KickOutPlayer extends KickOutPlayer {}
export interface Payload$LeaveWaitingPage extends LeaveWaitingPage {}
export interface Payload$Lobby extends Lobby {}
export interface Payload$Ping extends Ping {}
export interface Payload$Play extends Play {}
export interface Payload$Played extends Played {}
export interface Payload$Pong extends Pong {}
export interface Payload$Play extends Play {}
export interface Payload$Ready extends Ready {}
export interface Payload$SearchResult extends SearchResult {}
export interface Payload$SelectVillage extends SelectVillage {}
export interface Payload$Settings extends Settings {}
export interface Payload$WatingPage extends WatingPage {}
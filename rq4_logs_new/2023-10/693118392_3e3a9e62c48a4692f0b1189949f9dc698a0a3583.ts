import type schema from './schema'
import type { StorageSchema } from '@verdant-web/common'
import type {
  Client as Storage,
  ClientDescriptorOptions as StorageInitOptions,
  ObjectEntity,
  ListEntity,
  Query,
  ServerSync,
  EntityFile,
  CollectionQueries
} from '@verdant-web/store'
export * from '@verdant-web/store'
export type Schema = typeof schema

interface Collection<
  Document extends ObjectEntity<any, any>,
  Snapshot,
  Init,
  Filter
> {
  put: (init: Init, options?: { undoable?: boolean }) => Promise<Document>
  delete: (id: string, options?: { undoable?: boolean }) => Promise<void>
  deleteAll: (ids: string[], options?: { undoable?: boolean }) => Promise<void>
  get: (id: string) => Query<Document>
  findOne: (filter: Filter) => Query<Document>
  findAll: (filter?: Filter) => Query<Document[]>
  findAllPaginated: (
    filter?: Filter,
    pageSize?: number
  ) => Query<Document[], { offset?: number }>
  findAllInfinite: (
    filter?: Filter,
    pageSize?: number
  ) => Query<Document[], { offset?: number }>
}

export class Client<Presence = any, Profile = any> {
  readonly accounts: CollectionQueries<Account, AccountInit, AccountFilter>

  sync: ServerSync<Profile, Presence>
  undoHistory: Storage['undoHistory']
  namespace: Storage['namespace']
  entities: Storage['entities']
  queryStore: Storage['queryStore']
  batch: Storage['batch']
  files: Storage['files']

  close: Storage['close']

  export: Storage['export']
  import: Storage['import']

  subscribe: Storage['subscribe']

  stats: () => Promise<any>
  /**
   * Resets all local data. Use with caution. If this replica
   * is synced, it can restore from the server, but if it is not,
   * the data will be permanently lost.
   */
  __dangerous__resetLocal: Storage['__dangerous__resetLocal']
}

// schema is provided internally. loadInitialData must be revised to pass the typed Client
interface ClientInitOptions<Presence = any, Profile = any>
  extends Omit<StorageInitOptions<Presence, Profile>, 'schema'> {
  /** WARNING: overriding the schema is dangerous. Use with caution. */
  schema?: StorageSchema
}

export class ClientDescriptor<Presence = any, Profile = any> {
  constructor(init: ClientInitOptions<Presence, Profile>)
  open: () => Promise<Client<Presence, Profile>>
  readonly current: Client<Presence, Profile> | null
  readonly readyPromise: Promise<Client<Presence, Profile>>
  readonly schema: StorageSchema
  readonly namespace: string
  close: () => Promise<void>
}
export type Account = ObjectEntity<
  AccountInit,
  AccountDestructured,
  AccountSnapshot
>

export interface AccountIdMatchFilter {
  where: 'id'
  equals: string
  order?: 'asc' | 'desc'
}

export interface AccountIdRangeFilter {
  where: 'id'
  gte?: string
  gt?: string
  lte?: string
  lt?: string
  order?: 'asc' | 'desc'
}

export interface AccountIdStartsWithFilter {
  where: 'id'
  startsWith: string
  order?: 'asc' | 'desc'
}

export interface AccountUserIdMatchFilter {
  where: 'userId'
  equals: string
  order?: 'asc' | 'desc'
}

export interface AccountUserIdRangeFilter {
  where: 'userId'
  gte?: string
  gt?: string
  lte?: string
  lt?: string
  order?: 'asc' | 'desc'
}

export interface AccountUserIdStartsWithFilter {
  where: 'userId'
  startsWith: string
  order?: 'asc' | 'desc'
}

export interface AccountNameMatchFilter {
  where: 'name'
  equals: string
  order?: 'asc' | 'desc'
}

export interface AccountNameRangeFilter {
  where: 'name'
  gte?: string
  gt?: string
  lte?: string
  lt?: string
  order?: 'asc' | 'desc'
}

export interface AccountNameStartsWithFilter {
  where: 'name'
  startsWith: string
  order?: 'asc' | 'desc'
}
export type AccountFilter =
  | AccountIdMatchFilter
  | AccountIdRangeFilter
  | AccountIdStartsWithFilter
  | AccountUserIdMatchFilter
  | AccountUserIdRangeFilter
  | AccountUserIdStartsWithFilter
  | AccountNameMatchFilter
  | AccountNameRangeFilter
  | AccountNameStartsWithFilter

export type AccountDestructured = {
  id: string
  userId: string
  name: string
  currency: string
  balance: number
}
export type AccountInit = {
  id?: string
  userId: string
  name: string
  currency: string
  balance?: number
}
export type AccountSnapshot = {
  id: string
  userId: string
  name: string
  currency: string
  balance: number
}
/** Account sub-object types */

export type AccountId = string
export type AccountIdInit = AccountId | undefined
export type AccountIdSnapshot = AccountId
export type AccountIdDestructured = AccountId
export type AccountUserId = string
export type AccountUserIdInit = AccountUserId
export type AccountUserIdSnapshot = AccountUserId
export type AccountUserIdDestructured = AccountUserId
export type AccountName = string
export type AccountNameInit = AccountName
export type AccountNameSnapshot = AccountName
export type AccountNameDestructured = AccountName
export type AccountCurrency = string
export type AccountCurrencyInit = AccountCurrency
export type AccountCurrencySnapshot = AccountCurrency
export type AccountCurrencyDestructured = AccountCurrency
export type AccountBalance = number
export type AccountBalanceInit = AccountBalance | undefined
export type AccountBalanceSnapshot = AccountBalance
export type AccountBalanceDestructured = AccountBalance
import _ from "lodash"

import { gystEntriesFromResponse } from "~/src/cli/store/loader"

// Types
import {
  GystEntryResponseSuccess
} from "~/src/common/types/pages/main"
import { GystEntryWrapper } from "~/src/common/types/pages/main"

const DB_NAME = "feedgal"
const OBJECT_STORE_NAME = "rss-feeds"

export async function storeFeeds(response:GystEntryResponseSuccess):Promise<void> {
  const entries = gystEntriesFromResponse(response)
  const db = await initObjectStore()

  await Promise.all(entries.map(entry => {
    return new Promise(async (res, rej) => {
      const ost = db.transaction(OBJECT_STORE_NAME, "readwrite").objectStore(OBJECT_STORE_NAME)
      // Refer to the `index` of the object store
      const new_entry = { id: entry.entry.id, entry: <GystEntryWrapper>entry }

      const request:IDBRequest = ost.add(new_entry)

      request.onsuccess = ({ target }) => {
        const key_path = (<IDBRequest> target).result
      }
      request.onerror = (event:any) => {
        const error = _.get(event, "target.error")
        if(error) {
          const code = error.code
          if(code == 0) {
            // ConstraintError
          }
          else if(error.code == 20) {
            // AbortError
            /**
             * 2020-07-11 19:38
             * 
             * Some error occurred ahead of this request, and the rest of the actions that
             * use the same transaction will run in to this error.
             * 
             * Reproducing the error. Instead of creating a transaction for each entry,
             * create one transaction and try to add all entries with it. When one of them
             * throws an error (whose code isn't 20) will throw this error.
             */
          }
          else {
            rej(error)
          }
          return
        }

        rej(event)
      }
    })
  }))
}

const MAX_OLDER_FEEDS__COUNT = 10
export async function getOlderFeeds(entry_id:string):Promise<GystEntryWrapper[]> {
  const db = await initObjectStore()
  const ost = db.transaction(OBJECT_STORE_NAME).objectStore(OBJECT_STORE_NAME);

  const target_entry = await wrap(ost.index("byId").openCursor(entry_id))
  const primary_key = target_entry.primaryKey
  const key_path_range:IDBKeyRange = IDBKeyRange.lowerBound(primary_key, true)
  const result:GystEntryWrapper[] = []

  let count = 0
  const cursor_request = ost.openCursor(key_path_range)
  await new Promise((res, rej) => {
    cursor_request.onsuccess = () => {
      const cursor = cursor_request.result
      if(cursor == undefined || count >= MAX_OLDER_FEEDS__COUNT) {
        res(result)
        return
      }

      result.push(<GystEntryWrapper>cursor.value.entry)
      count++
      cursor.continue();
    }
  })
  return result
}

function initObjectStore():Promise<IDBDatabase> {
  return new Promise((res, rej) => {
    Object.assign(
      indexedDB.open(DB_NAME, 1),
      {
        onupgradeneeded: (event:Event) => {
          const db = (<IDBOpenDBRequest> event.target!).result
          /**
           * 2020-07-12 09:57
           * 
           * Apparently, entries are sorted by the `keyPath` when being inserted. Tested with
           * `getAll` and cursor.
           */
          const rss_feeds = db.createObjectStore(OBJECT_STORE_NAME, { autoIncrement: true })
          rss_feeds.createIndex("byId", ["id"], { unique: true })
        },
        onsuccess: (event:Event) => {
          const db = (<IDBOpenDBRequest> event.target!).result
          res(db)
        },
        onerror: (error:any) => {
          rej(error)
        }
      }
    )
  })
}

function wrap(req:IDBRequest):Promise<any> {
  return new Promise((res, rej) => {
    req.onsuccess = (event) => res((<IDBRequest>event.target).result )
    req.onerror = (event) => rej(event.target)
  })
}
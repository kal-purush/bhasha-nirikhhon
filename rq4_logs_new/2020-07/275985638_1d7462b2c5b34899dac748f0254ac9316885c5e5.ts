import _ from "lodash"
import moment from "moment"
import { getModule } from "vuex-module-decorators"

import * as RssStorage from "~/src/cli/store/loader/rss-indexeddb"
import Store from "~/store/loader.ts"
import { getParam, gystEntriesFromResponse, iterateLoadStatus } from "./utility"
import { ALL_LOADED } from "~/src/common/warning-error"

// Types
import {
  GystEntryResponseSuccess,
  GystEntryResponseError,
  SuiteEntryIdObject,
  LoadStatusServiceSetting,
  LoadStatusSettingValue,
  SuiteEntryCache,
  ServicePaginationReqParam,
  GystEntryWrapper as GystEntryWrapperType,
  PaginationData,
} from "~/src/common/types/pages/main"
import { WarningName, WarningObject } from "~/src/common/types/common/warning-error"

export type PaginationParamSuggestion = {
  suggested: boolean
  reasons?: { [reason:string]: boolean }
  pagination_req_param?: ServicePaginationReqParam
}

const LOAD_ENTRIES_NUM = 10
const ENOUGH_LOADED = 10

export class Loader {
  /**
   * 2020-07-13 12:39
   * 
   * REMINDER. Use Vuex to prevent having to pass the instance to a child component or
   * access a parent component's field to get data out of it. With Vuex (with TypeScript
   * and decorator), you don't have to deal with `this.$parent` or `@Prop`, but just use
   * `@State` or `@Getter` to retrieve data.
   * 
   * Creating this class because I hate it how you can't call another method within a
   * method in Vuex (or VuexModule, to be exact). When needed by the 'parent' or 'child'
   * (in quotes because Vuex already removes this distinction), you just instantiate
   * this class.
   * 
   * No need to worry about 'unsynced data' between parent and child because this `store`
   * will be the one and only field which is a Vuex store.
   */
  store:Store

  constructor(vuex_store:any) {
    this.store = getModule(Store, vuex_store)
  }

  public startInitRequest() {
    this.store.deriveSuiteEntriesCache()
    this.store.load_status.forEach(service_setting => {
      this.setServiceSettingIsLoading(service_setting._id, true)
    })
  }

  public async storeResponse(response:GystEntryResponseSuccess) {
    this.store.mutateCache([
      response,
      (cache:SuiteEntryCache) => {
        cache.pagination_data = response.pagination_data
        cache.entries = response.entries
        cache.warning = response.warning
      }
    ])

    if(response.service_id == "rss") {
      await RssStorage.storeFeeds(response)
    }

    this.store.mutateLoadStatus([
      response,
      (suite_entry:LoadStatusServiceSetting|LoadStatusSettingValue) => {
        if(response.warning) {
          suite_entry.warning = response.warning
        }
        else {
          delete suite_entry.warning
        }
        delete suite_entry.error

        suite_entry.total += response.entries.length
      }
    ])

    // Handles service setting and setting value dependence
    this.setSuiteEntryIsLoading(response, false)

    this.store.mutateEntries((store) => {
      const entries = gystEntriesFromResponse(response)
      store.preloaded_entries = store.preloaded_entries.concat(entries)
      store.preloaded_entries.sort((a,b) => moment(b.entry.datetime_info).isAfter(moment(a.entry.datetime_info)) ? 1 : -1)
    })
  }

  public storeError(response:GystEntryResponseError) {
    this.store.mutateLoadStatus([
      response,
      (suite_entry:LoadStatusServiceSetting|LoadStatusSettingValue) => {
        suite_entry.error = response.error
      }
    ])

    // Handles service setting and setting value dependence
    this.setSuiteEntryIsLoading(response, false)
  }

  public isNonLoaded() {
    return this.store.loaded_entries.length == 0
  }

  public isAllLoaded() {
    const is_all_loaded = this.store.load_status.every(service_setting => service_setting.is_loading == false)
    return is_all_loaded
  }

  /**
   * Can only load entries from preloaded_entries which is filled in
   * `storeResponse`.
   */
  public loadEntries() {
    // Load at least X feeds from each suite entry

    // Load the rest from `preloaded_entries`
    this.store.mutateEntries((store) => {
      const entries:GystEntryWrapperType[] = []

      for(let a=0; a < store.preloaded_entries.length; a++) {
        const entry = store.preloaded_entries[a]
        entries.push(entry)
        store.preloaded_entries.splice(a, 1)

        if(entries.length >= LOAD_ENTRIES_NUM) {
          break
        }
      }

      store.loaded_entries = store.loaded_entries.concat(entries)
    })
  }

  /**
   * Can be used for automatically loading pagination on getting response.
   */
  public async suggestPaginationParam(suite_entry_ids:SuiteEntryIdObject):Promise<PaginationParamSuggestion> {
    const { service_setting_id, setting_value_id } = suite_entry_ids

    const cache = this.store.suite_entries_cache.find(entry => {
      return entry.service_setting_id == service_setting_id &&
        entry.setting_value_id == setting_value_id
    })!
    const pagination_data:ServicePaginationReqParam = {
      service_id: cache.service_id,
      service_setting_id: cache.service_setting_id,
      oauth_connected_user_entry_id: cache.oauth_connected_user_entry_id,
      pagination_data: cache.pagination_data,
      setting_value: cache.setting_value,
      setting_value_id: cache.setting_value_id,
      warning: cache.warning
    }

    /**
     * 2020-07-13 14:20
     * 
     * Leaving this here because it's only used in this method
     */
    const service_setting:LoadStatusServiceSetting = this.store.load_status.find(entry => entry._id == service_setting_id)!
    let setting_value:LoadStatusSettingValue|undefined = undefined
    if(service_setting.uses_setting_value) {
      setting_value = service_setting.setting_values.find(entry => entry._id == setting_value_id)!
    }

    const service_setting_warning_name = _.get(service_setting.warning, "name")
    const setting_value_warning_name = _.get(setting_value?.warning, "name")
    /**
     * 2020-06-09 10:08
     * 
     * Don't request for next pagination when there is `RATE_LIMIT` warning because it will result in
     * somewhat 'endless loop' of making request and resposne with `RATE_LIMIT` warning until the
     * outside service finally returns a proper response or some error/warning that is not converted
     * into `RATE_LIMIT` by the gyst server.
     */
    const rate_limit_warning = [service_setting_warning_name, setting_value_warning_name].some(name => name == <WarningName> "RATE_LIMIT")
    const all_loaded_warning = [service_setting_warning_name, setting_value_warning_name].some(name => name == <WarningName> "ALL_LOADED")
    const error_exists = service_setting.error != undefined && setting_value?.error != undefined
    // const is_enough_loaded = cache.entries.length >= ENOUGH_LOADED
    const is_enough_loaded = (() => {
      let count = 0
      return this.store.preloaded_entries.some(entry => {
        const detail = entry.suite_entry
        if(detail.service_setting_id == service_setting_id && detail.setting_value_id == setting_value_id) {
          count++
        }

        return count >= ENOUGH_LOADED
      })
    })()

    /**
     * 2020-07-13 14:29
     * 
     * It's like `rate_limit_warning && all_loaded_warning && error_exists && is_enough_loaded`. And it's so
     * because when you evaluate one condition by one, and it evaluates to 'true' then you could `return`.
     */
    // const handled_on_client_side = service_setting.service_id == "rss"
    let suggested = [rate_limit_warning, all_loaded_warning, error_exists, is_enough_loaded].every(cond => cond == false)
    const reasons = { rate_limit_warning, all_loaded_warning, error_exists, is_enough_loaded }

    /**
     * 2020-07-14 13:25
     * 
     * RSS makes `suggested == false`. Assume that this will not be processed normally by the caller.
     * And handle RSS pagination here.
     */
    const is_rss = service_setting.service_id == "rss"
    if(is_rss && suggested) {
      // this is ASYNC
      this.handleRssPagination([pagination_data])
      suggested = false
    }

    /**
     * 2020-07-13 14:16
     * 
     * Should be evaluated with the `suggested` property the output of this method on the
     * caller side.
     * 
     * Remove this comment and the following line after implementing
     */
    // const count_loaded_entries = "entries" in response ? response.entries.length : 0

    return { suggested, pagination_req_param: pagination_data, reasons }
  }

  public async suggestAllPaginationParams():Promise<PaginationParamSuggestion[]> {
    const suggestions = await Promise.all(
      this.store.suite_entries_cache.map(async cache => await this.suggestPaginationParam(cache))
    )
    return suggestions
  }

  setSuiteEntryIsLoading(suite_entry_ids:SuiteEntryIdObject, is_loading:boolean) {
    const { setting_value_id, service_setting_id } = suite_entry_ids
    if(setting_value_id) {
      this.setSettingValueIsLoading(suite_entry_ids, is_loading)
    }
    else {
      this.setServiceSettingIsLoading(service_setting_id, is_loading)
    }
  }

  /**
   * Updates underlying setting value if exists
   */
  setServiceSettingIsLoading(service_setting_id:string, is_loading:boolean) {
    this.store.mutateLoadStatus([
      { service_setting_id },
      (service_setting:LoadStatusServiceSetting) => {
        service_setting.is_loading = is_loading
        service_setting.setting_values.forEach(setting_value => setting_value.is_loading = is_loading)
      }
    ])
  }

  /**
   * Updates its service setting if all of its setting value finished loading
   */
  setSettingValueIsLoading(suite_entry_ids:SuiteEntryIdObject, is_loading:boolean) {
    this.store.mutateLoadStatus([
      suite_entry_ids,
      (setting_value:LoadStatusSettingValue, service_setting:LoadStatusServiceSetting) => {
        setting_value.is_loading = is_loading
        
        /**
         * Specially handle "all loaded". Not checking whether the service setting uses
         * setting value because this method already assumes that it does.
         */
        const all_loaded = service_setting.setting_values.every(setting_value => setting_value.is_loading == false)
        if(all_loaded) {
          service_setting.is_loading = false
        }
      }
    ])
  }

  /**
   * 2020-07-12 09:06
   * 
   * Alternative solution would be to insert ALL feeds in the indexedDB into the `preloaded_entries`
   * on getting init RSS feeds.
   */
  async handleRssPagination(pagination_req_data:ServicePaginationReqParam[]) {
    if(pagination_req_data.length == 0) return
    if(pagination_req_data.length > 1) {
      console.warn(`Something wrong with RSS pagination. Unexpected number of pagination_req_data`)
    }

    const data = pagination_req_data[0]
    const last_entry_key_path = data.pagination_data!.old
    const records:RssStorage.Record[] = await RssStorage.getOlderFeeds(last_entry_key_path, (cursor) => {
      const value:GystEntryWrapperType = cursor.value.entry
      const index_in_preloaded = this.store.preloaded_entries.findIndex(entry => entry.entry.id == value.entry.id)
      const index_in_loaded = this.store.loaded_entries.findIndex(entry => entry.entry.id == value.entry.id)
      return index_in_preloaded == -1 && index_in_loaded == -1
    })
    const entries = records.map(record => record.entry.entry)

    let pagination_data!:PaginationData
    let warning:WarningObject|undefined = undefined
    const response:GystEntryResponseSuccess = {
      service_id: data.service_id,
      service_setting_id: data.service_setting_id,
      setting_value_id: data.setting_value_id,
      setting_value: data.setting_value,
      entries,
      pagination_data,
    }
    if(entries.length == 0) {
      response.pagination_data = { old: last_entry_key_path, new: null }
      response.warning = ALL_LOADED()
    }
    else {
      response.pagination_data = { old: records.slice(-1)[0].key, new: null }
    }

    await this.storeResponse(response)
  }
}
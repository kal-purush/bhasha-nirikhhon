import { SessionSocketEventHandler } from "./session"
import { throwControlledError, convertError } from "../common/convert-error"

// Types
import {
  GystEntryResponseSuccess,
  GystEntryResponseError
} from "~/src/common/types/pages/main"
import { ErrorName, ErrorObject } from "~/src/common/types/common/warning-error"
import { EntriesResult } from "~/src/server/method-collection/common/services/base/types"
import { FlattenedLoaderParam } from "~/src/server/method-collection/get-entries-init"

export abstract class GetEntriesBaseSocketHandler extends SessionSocketEventHandler {
  message_base:string
  constructor(message_base:string) {
    super()
    this.message_base = message_base
  }

  respond(param:FlattenedLoaderParam, entries_result:EntriesResult) {
    const { service_id, service_setting_id, setting_value_id, setting_value, oauth_connected_user_entry_id } = param

    this.socket.emit(`${this.message_base}-response`, <GystEntryResponseSuccess>{
      service_id,
      oauth_connected_user_entry_id,
      setting_value,
      service_setting_id,
      setting_value_id,
      entries: entries_result.entries,
      pagination_data: entries_result.pagination_data,
      service_response: entries_result.service_response,
      warning: entries_result.warning
    })
  }

  respondError(param:FlattenedLoaderParam, error:ErrorObject) {
    const { service_id, service_setting_id, setting_value_id, setting_value, oauth_connected_user_entry_id } = param

    this.socket.emit(`${this.message_base}-error`, <GystEntryResponseError>{
      service_id,
      oauth_connected_user_entry_id,
      setting_value,
      service_setting_id,
      setting_value_id,
      error,
    })
  }

  abstract async getIterable():Promise<any[]>
  abstract async getEntriesResult(param:any):Promise<EntriesResult>

  async handleImpl() {
    const params:any[] = await this.getIterable()

    await Promise.all(
      params.map(async param => {    
        const error = await throwControlledError(param)
        if(error) {
          return this.respondError(param, error)
        }

        try {
          const entries_result:EntriesResult = await this.getEntriesResult(param)
          return this.respond(param, entries_result)
        }
        catch(e) {
          const error = await convertError(param, e)
          return this.respondError(param, error)
        }
      })
    )
  }
}
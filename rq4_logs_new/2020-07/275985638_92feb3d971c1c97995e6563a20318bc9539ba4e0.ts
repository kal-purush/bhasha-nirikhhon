import _ from "lodash"
import { ErrorOnRefreshRequest } from "oauth-module-suite"

import { getServiceInfo } from "~/src/server/method-collection"
import { isTokenError as _isTokenError } from "~/src/server/lib/oauth-cred-module-helper"
import { isSettingValueError as _isSettingValueError } from "~/src/server/method-collection/is-setting-value-error"

import {
  GOOGLE_AUTHORIZATION_ERROR,
  RIOT_API_ERROR,
  INVALID_SETTING_VALUE,
  TOKEN_MARKED_ERROR,
} from "~/src/common/warning-error"

// Models
import { oauth_connected_user_storage } from "~/src/server/model-collection/models/oauth-connected-user"
import { setting_value_storage } from "~/src/server/model-collection/models/setting-value"

// Types
import {
  LoadEntryParamDetail
} from "~/src/common/types/pages/main"
import { Warning, Error } from "~/src/common/types/common/warning-error"
import { EntriesResult, ServiceInfo } from "~/src/server/method-collection/common/services/base/types"

/**
 * 2020-06-18 17:28 
 * 
 * The thrown error ARE actually javascript Error compatible because they have `name` and `message`
 * properties
 */
export async function handleError(
  detail:LoadEntryParamDetail,
  e:any
):Promise<Error> {
  const { service_id, setting_value_id, service_setting_id, oauth_connected_user_entry_id } = detail

  const service_info = getServiceInfo(service_id)
  const is_token_error = isTokenError(service_info, e)
  const is_setting_value_error = isSettingValueError(service_info, e)

  if(is_token_error) {
    await oauth_connected_user_storage.setError(oauth_connected_user_entry_id!, true)

    return TOKEN_MARKED_ERROR
  }
  else if(is_setting_value_error) {
    await setting_value_storage.invalidateSettingValue(setting_value_id!)

    return INVALID_SETTING_VALUE
  }
  else {
    if("response" in e ) {
      const status = e.response.status
      
      if(service_id == "league-of-legends") {
        if(status == 403) {
          return <Error> RIOT_API_ERROR
        }
      }
      else if(service_info.oauth_service_id == "google") {
        /**
         * 2020-06-29 22:10
         * 
         * Note. There are many 403 errors. But all errors regarding granting access has
         * `e.response.data.error.status == PERMISSION_DENIED`
         */
        if(status == 403 && _.get(e, "response.data.error.status") == "PERMISSION_DENIED") {
          return <Error> GOOGLE_AUTHORIZATION_ERROR
        }
      }
    }
  }

  throw e
}

export async function throwControlledError({ service_id, oauth_connected_user_entry_id, setting_value_id }:LoadEntryParamDetail):Promise<Error|undefined> {
  if(oauth_connected_user_entry_id) {
    const is_error = await oauth_connected_user_storage.isErrorWithUserUid(service_id, oauth_connected_user_entry_id)
    if(is_error) {
      return TOKEN_MARKED_ERROR
    }
  }
  
  if(setting_value_id) {
    const is_invalid = await setting_value_storage.isInvalid(setting_value_id)
    if(is_invalid) {
      return INVALID_SETTING_VALUE
    }
  }
}

function isTokenError(service_info:ServiceInfo, e:Error):boolean {  
  /**
   * If it's a token error, it must have gone through `refreshTokenifFail` which throws an 
   * error that wraps the original error in `original_error`.
   */
  if("original_error" in e == false || service_info.is_oauth == false) return false

  /**
   * Error while refreshing token definitely has a problem with tokens.
   */
  if(e instanceof ErrorOnRefreshRequest) return true

  const service_id = service_info.oauth_service_id!
  return _isTokenError(service_id, (<any> e).original_error)
}

function isSettingValueError(service_info:ServiceInfo, e:Error):boolean {  
  if(service_info.uses_setting_value == false) return false

  const service_id = service_info.service_id
  return _isSettingValueError(service_id, e)
}
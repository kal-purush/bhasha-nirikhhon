import _ from "lodash"

import { collection } from "../common/services"

// Models
import { service_setting_storage } from "~/src/server/model-collection/models/service-setting"
import { oauth_connected_user_storage } from "~/src/server/model-collection/models/oauth-connected-user"

// Types 
import { ServiceInfo } from "../common/services/base/types"
import { ServiceSetting } from "~/src/common/types/models/service-setting"
import { OAuthConnectedUser } from "~/src/common/types/models/oauth-connected-user"

export type CallbackParam = { service_setting:ServiceSetting, service_info:ServiceInfo|undefined, oauth_account:OAuthConnectedUser|undefined }
type Callback = (param:CallbackParam) => Promise<void>
export type IncludeOption = { service_info?:boolean, oauth_account?:boolean }

/**
 * 2020-07-05 15:19
 * 
 * Would accept `suite_id` in the future
 */
export async function iterateServiceSettings(user_id:string, cb:Callback, option?:IncludeOption) {
  // const DEFAULT_INCLUDE_OPTION:IncludeOption = { service_info: true, oauth_account: true }
  const include_service_info = _.get(option, "service_info", true)
  const include_oauth_account = _.get(option, "oauth_account", false)

  const service_settings = await service_setting_storage.getAllServiceSettingsForUserId(user_id)

  await Promise.all(
    service_settings.map(async entry => {
      let service_info:ServiceInfo|undefined
      let oauth_account:OAuthConnectedUser|undefined

      const service_setting:ServiceSetting = entry.toJSON()
      const service_id = service_setting.service_id

      if(include_service_info) {
        service_info = collection[service_id].getServiceInfo()
      }

      if(include_oauth_account && service_setting.oauth_connected_user_entry_id) {
        oauth_account = <OAuthConnectedUser> <any> await oauth_connected_user_storage.getEntry(service_setting.oauth_connected_user_entry_id)
      }

      await cb({ service_setting, service_info, oauth_account })
    })
  )
}
import { getServiceSetting } from "~/src/server/gyst-server/common/gyst-suite"

// Types
import { OAuthConnectedUser } from "~/src/common/types/pages/settings-accounts"
import { OAuthConnectedUser as ModelOAuthConnectedUser } from "~/src/common/types/models/oauth-connected-user"
import { ServiceSetting } from "~/src/common/types/common/suite"

// Models
import { service_setting_storage } from "~/src/server/model-collection/models/service-setting"
import { oauth_connected_user_storage } from "~/src/server/model-collection/models/oauth-connected-user"

export async function getOAuthConnectedAccounts(user_id:string):Promise<OAuthConnectedUser[]> {
  const connected_account_entries = (await oauth_connected_user_storage.getAllEntriesForUserId(user_id)).map(entry => <ModelOAuthConnectedUser> entry.toJSON())
  const oauth_connected_accounts:OAuthConnectedUser[] = await Promise.all(
    connected_account_entries.map(async entry => {
      return <OAuthConnectedUser> {
        ...entry,
        used_in: await getServiceSettingsUsedByOAuthAccount(entry._id)
      }
    })
  )

  return oauth_connected_accounts
}

/**
 * 2020-07-06 19:28
 * 
 * Note that it's different from `getSuiteServiceSettingsForUserId` because it only retrieves service settings
 * that relies on the passed `oauth_user_entry_id`. In other words, this function won't return service settings
 * of non-oauth services.
 */
async function getServiceSettingsUsedByOAuthAccount(oauth_user_entry_id:string):Promise<ServiceSetting[]> {
  const service_setting_ids = (await service_setting_storage.getAllServiceSettingsForOAuthConnectedUserEntryId(oauth_user_entry_id)).map(entry => entry._id)
  const service_settings:ServiceSetting[] = await Promise.all(
    service_setting_ids.map(id => getServiceSetting(id))
  )

  return service_settings
}
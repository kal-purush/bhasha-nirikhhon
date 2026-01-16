import { getSuiteServiceSettingsForUserId } from "~/src/server/gyst-server/common/gyst-suite"

// Types
import {
  ClientSideField,
  LoadStatusServiceSetting
} from "~/src/common/types/pages/main"

export async function getLoadStatus(user_id:string) {
  const _load_status = await getSuiteServiceSettingsForUserId(user_id)
  const load_status = _load_status.map(_service_setting => {
    const service_setting:LoadStatusServiceSetting = <any> _service_setting
    Object.assign(service_setting, <ClientSideField> {
      is_loading: false,
      total: 0
    })

    if(service_setting.uses_setting_value) {
      service_setting.setting_values.forEach(setting_value => {
        Object.assign(setting_value, <ClientSideField> {
          is_loading: false,
          total: 0
        })
      })
    }

    return service_setting
  })

  return load_status
}
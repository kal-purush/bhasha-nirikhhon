import { refreshTokenIfFailOAuthServiceId } from "~/src/server/method-collection/common/refresh-token-if-fail"

import { collection } from "../../common/services"

type ServiceIdParam = { service_id:string, oauth_user_entry_id:string }
type OAuthServiceIdParam = { oauth_service_id:string, oauth_user_entry_id:string }
type Callback = ((e:any, result:any) => Promise<any>) | ((e:any, result:any) => any)

export abstract class OAuthMethodBase {
  oauth_service_id:string
  oauth_user_entry_id:string

  constructor(param:ServiceIdParam|OAuthServiceIdParam) {
    if((<ServiceIdParam>param).service_id) {
      const service_id = (<ServiceIdParam>param).service_id
      const service_info = collection[service_id].getServiceInfo()
      this.oauth_service_id = service_info.oauth_service_id!
    }
    else {
      this.oauth_service_id = (<OAuthServiceIdParam> param).oauth_service_id
    }

    this.oauth_user_entry_id = param.oauth_user_entry_id
  }

  abstract runImpl(token_data:any):Promise<any>

  async run(handler:Callback):Promise<void> {
    try {
      let result:any
      await refreshTokenIfFailOAuthServiceId(this.oauth_service_id, this.oauth_user_entry_id, async (token_data:any) => {
        result = await this.runImpl(token_data)
      })
      await handler(undefined, result)
    }
    catch(e) {
      await handler(e, undefined)
    }
  }
}
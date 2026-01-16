import { BaseApi } from '@api/baseApi'
import { ClientID, OauthSignInRequest } from './types'
import { Routes } from '@routes/constants'
import {
    generateRandomStateStr,
    removeState,
    saveState,
} from '@utils/authentication/oauth'
import { closePopup } from '@utils/ui/popup'

const BaseURL = 'oauth'
const OAuthURL = 'https://oauth.yandex.ru/authorize'

const Port = '3000'
const TestUrl = `http://localhost:${Port}`

export const ErrCodeParam = 'error'
export const ErrDescParam = 'error_description'
export const StateParam = 'state'
export const ErrorState =
    'параметр "Строка состояния" не совпадает с переданным на сервер авторизации'

export class OAuthApi extends BaseApi {
    static readonly redirectUri = `${TestUrl}${Routes.OAuth}`
    private clientID = 'dc45f725fcd94bb2a11472239798fc42'

    constructor() {
        super(BaseURL)
    }

    cleanup() {
        closePopup()
        removeState()
    }

    async getServiceId(abort?: AbortSignal) {
        const params = new URLSearchParams([
            ['redirect_uri', OAuthApi.redirectUri],
        ])

        const config = abort ? { signal: abort, params } : { params }

        const { data } = await this.client.get<ClientID>(
            'yandex/service-id',
            config
        )

        return data
    }

    async exchangeCodeForToken(req: OauthSignInRequest) {
        const { data } = await this.client.post<string>('yandex', req)
        return data
    }

    async urlYandex(abort: AbortSignal) {
        // const id = await this.getServiceId(abort)
        const id = {
            service_id: this.clientID,
        }

        const param = this.urlParams(id)
        const url = `${OAuthURL}?${param}`

        return url
    }

    private urlParams(clientId: ClientID) {
        const responseType = 'code'

        const state = generateRandomStateStr()
        saveState(state)

        const params = new URLSearchParams([
            ['response_type', responseType],
            ['client_id', clientId.service_id],
            ['redirect_uri', OAuthApi.redirectUri],
            ['state', state],
        ])

        return params.toString()
    }
}

export const oAuthApi = new OAuthApi()
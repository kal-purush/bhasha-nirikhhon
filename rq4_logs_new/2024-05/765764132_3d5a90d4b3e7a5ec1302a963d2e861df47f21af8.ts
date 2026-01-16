import { BaseApi } from '@api'
import { UserThemeDTO } from '@api/userTheme/types'

export class UserThemeApi extends BaseApi {
    constructor() {
        super('userTheme', false)
    }

    getTheme = async (id: number) => {
        const { data } = await this.client.get<UserThemeDTO>(this.withUrl(), {
            params: {
                ownerId: id,
            },
        })
        return data
    }

    setTheme = async (userId: number, theme: string) => {
        const { data } = await this.client.post<UserThemeDTO>(this.withUrl(), {
            theme,
            ownerId: userId,
        })

        return data
    }
}

export const userThemeApi = new UserThemeApi()
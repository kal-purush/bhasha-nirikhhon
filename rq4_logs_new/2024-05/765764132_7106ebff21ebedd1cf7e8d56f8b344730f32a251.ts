import { BaseApi } from '@api/baseApi'
import { ReactionDTO, addReactionParams } from './types'

export class ReactionApi extends BaseApi {
    constructor() {
        super('reaction', false)
    }

    getReactions = async (topicIds: number[]) => {
        const { data } = await this.client.post<Record<number, ReactionDTO[]>>(
            this.withUrl(),
            {
                topicIds,
            }
        )
        return data
    }

    addReaction = async ({ topicId, userId, emojiId }: addReactionParams) => {
        const { data } = await this.client.post<string>(
            this.withUrl(`/${topicId}/${userId}`),
            { emojiId }
        )
        return data
    }
}

export const reactionApi = new ReactionApi()
import { BaseApi } from '@api/baseApi'
import { Topics } from '@store/forum/types'
import { Topic } from '@store/topic/types'
import {
    CreateCommentDTO,
    CreateResponse,
    CreateTopicDTO,
    TopicsGetParams,
} from './types'

export class ForumApi extends BaseApi {
    constructor() {
        super('forum', false)
    }

    getTopics = async (params: TopicsGetParams) => {
        const { data } = await this.client.get<Topics[]>(this.withUrl(), {
            params,
        })
        return data
    }

    getTopic = async (id: string) => {
        const { data } = await this.client.get<Topic>(this.withUrl(id))
        return data
    }

    createTopic = async (topicData: CreateTopicDTO) => {
        const { data } = await this.client.post<CreateResponse>(
            this.withUrl(),
            topicData
        )
        return data
    }

    createComment = async (commentData: CreateCommentDTO) => {
        const { data } = await this.client.post<CreateResponse>(
            this.withUrl('comment'),
            commentData
        )
        return data
    }
}

export const forumApi = new ForumApi()
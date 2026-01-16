import { exists } from '../utils/CommonFunc'
import { reactionService } from '../services/reaction.service'
import type { Request, Response } from 'express'

const responseOK = 'Ok'

export class ReactionController {
    static async getByTopic(req: Request, res: Response) {
        const topicId = Number(req.params.topicId)
        const reactions = await reactionService.getByTopic(topicId)
        res.json(reactions)
    }

    static async getByManyTopics(req: Request, res: Response) {
        const { body } = req
        const idArray = body.topicIdArray as number[]
        const topicsToReactions = await reactionService.getByTopics(idArray)
        res.json(Object.fromEntries(topicsToReactions))
    }

    static async addOnTopic(req: Request, res: Response) {
        const { body } = req
        const topicId = Number(req.params.topicId)
        const userId = Number(req.params.userId)
        const emojiId = Number(body.emojiId)
        body.topicId = topicId
        body.userId = userId
        body.emojiId = emojiId

        await reactionService.addOrUpdate(body)

        res.send(responseOK)
    }

    static async getAvailableEmoticons(req: Request, res: Response) {
        const category = getParam(req, 'cat')
        const list = await reactionService.getEmoticons(category)
        res.json(list)
    }
}

function getParam(req: Request, param: string) {
    if (!exists(req.query[param])) {
        return undefined
    }

    return req.query[param] as string
}
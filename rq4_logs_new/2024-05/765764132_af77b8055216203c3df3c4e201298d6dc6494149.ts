import type { Request, Response } from 'express'
import { commentService } from '../services/comment.service'
import { validationResult } from 'express-validator'

export class CommentController {
    public static create = async (request: Request, response: Response) => {
        const errors = validationResult(request)
        if (!errors.isEmpty()) {
            response.status(400).json({ errors: errors.array() })
            return
        }

        const { body } = request
        await commentService.create(body)
        response.status(201).send('Ok')
    }

    public static findAll = async (request: Request, response: Response) => {
        const { query } = request
        const data = await commentService.findByParams(query)

        response.send(data)
    }
}
import type { Request, Response } from 'express'
import { validationResult } from 'express-validator'
import { themeService } from '../services/theme.service'

export class ThemeController {
    public static create = async (request: Request, response: Response) => {
        const errors = validationResult(request)
        if (!errors.isEmpty()) {
            response.status(400).json({ errors: errors.array() })
            return
        }

        const { body } = request
        const theme = await themeService.findOrCreate(body)
        response.status(201).send(theme)
    }

    public static findAll = async (request: Request, response: Response) => {
        const { query } = request
        const userTheme = await themeService.findAll(query)
        response.send(userTheme)
    }

    public static findById = async (request: Request, response: Response) => {
        const errors = validationResult(request)
        if (!errors.isEmpty()) {
            response.status(400).json({ errors: errors.array() })
            return
        }

        const { params } = request
        const userTheme = await themeService.findById(Number(params.id))
        response.send(userTheme)
    }
}
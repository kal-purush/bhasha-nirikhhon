import type { NextFunction, Request, Response } from 'express'
import { cookieService } from '../services/cookie.service'
import { resolveCookies } from '../utils/formatCookies'
import { ERRORS } from '../error/constants'
import { getErrorMessage } from '../error/getErrorMessage'

export const authMiddleware = async (
    req: Request,
    res: Response,
    next: NextFunction
) => {
    const { cookies } = req
    const cookieString = resolveCookies(cookies)

    if (!cookieString) {
        res.status(401).json(getErrorMessage(ERRORS.cookieNotValid))
        return
    }

    const cookie = await cookieService.findByCookie(cookieString)

    if (!cookie) {
        res.status(401).json(getErrorMessage(ERRORS.cookieNotValid))
        return
    }

    next()
}
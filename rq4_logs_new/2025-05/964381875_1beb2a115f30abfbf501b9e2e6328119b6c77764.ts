import { Response, NextFunction } from 'express'
import { UserEntity, UserRole } from '../entities/User'
import { Request as JWTRequest } from 'express-jwt'

/** 自定義錯誤 */
function generateError(errorCode: number): Error & { code: number } {
    const error = new Error() as Error & { code: number }
    error.code = errorCode
    return error
}

/** Middleware：驗證目前登入的使用者是否為廠商 */
export function isOrganizer(req: JWTRequest & { user?: UserEntity }, res: Response, next: NextFunction) {
    if (!req.auth || req.auth.role !== UserRole.ORGANIZER) {
        next(generateError(1016))
        return
    }
    next()
}
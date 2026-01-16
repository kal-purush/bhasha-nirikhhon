import { Response, NextFunction } from 'express'
import { Request as JWTRequest } from 'express-jwt'
import getLogger from '../utils/logger'
import { areaCache } from '../utils/areaCache'
import responseSend, { initResponseData } from '../utils/serverResponse'

const logger = getLogger('Admin')

/** 取得地區資料 */
export async function getAreas(req: JWTRequest, res: Response, next: NextFunction) {
    try {
        const results = await areaCache.getAreas();
        
        responseSend(initResponseData(res, 2000, { results }))
    } catch (error) {
        logger.error('取得地區資料錯誤:', error)
        next(error)
    }
}
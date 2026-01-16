import { Request, Response } from 'express'

import { constants } from 'sakiewka-crypto'
import { jsonResponse, errorResponse } from '../../../response'
import { createAuthTokenRequest } from '../../../models'
import validate from '../../../validate'

export const createAuthToken = (sakiewkaCrypto) => async (req: Request, res: Response) => {
  const { errors, body } = validate(req, createAuthTokenRequest, false)

  if (errors.length > 0) {
    return errorResponse(res, constants.API_ERROR.BAD_REQUEST, errors[0])
  }
  const { duration, ip, scope } = body
  const token = req.header('authorization')
  const backendResponse = await sakiewkaCrypto.user.createAuthToken(token, duration, ip, scope)

  jsonResponse(res, backendResponse)
}

export default createAuthToken
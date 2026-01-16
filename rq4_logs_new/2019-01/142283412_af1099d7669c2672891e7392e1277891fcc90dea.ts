import { Request, Response } from 'express'

import { jsonResponse, errorResponse } from '../../response'
import { maxTransferAmountRequest } from '../../models'
import sakiewkaCrypto from 'sakiewka-crypto'
import validate from '../../validate'

const { constants, wallet } = sakiewkaCrypto

const maxTransferAmount = async (req: Request, res: Response) => {
  const validationErrors = validate(req, maxTransferAmountRequest, true)

  if (validationErrors.length > 0) {
    return errorResponse(res, constants.API_ERROR.BAD_REQUEST, validationErrors[0])
  }
  const { feeRate, recipient } = req.query
  const backendResponse = await wallet.maxTransferAmount(req.header('authorization'), req.param('walletId'), feeRate, recipient)

  jsonResponse(res, backendResponse)
}

export default maxTransferAmount
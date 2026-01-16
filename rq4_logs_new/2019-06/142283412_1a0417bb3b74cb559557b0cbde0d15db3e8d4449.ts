import { Request, Response } from 'express'
import { Currency, constants } from 'sakiewka-crypto'
import { errorResponse, jsonResponse } from '../../../response'
import { editWalletRequest } from '../../../models'
import validate from '../../../validate'

const editWallet = (sakiewkaCrypto, currency: Currency) => async (req: Request, res: Response) => {
  const { wallet } = sakiewkaCrypto[currency]
  const { errors, body } = validate(req, editWalletRequest, true)

  if (errors.length > 0) {
    return errorResponse(res, constants.API_ERROR.BAD_REQUEST, errors[0])
  }

  const token = req.header('authorization')
  const { name } = body
  const responseData = await wallet.editWallet(token, req.params.id, name)

  jsonResponse(res, responseData)
}

export default editWallet
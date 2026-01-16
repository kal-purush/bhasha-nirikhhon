import { StatusCodes } from 'http-status-codes'
import { NextApiRequest, NextApiResponse } from 'next'
import { camelize, sortObjectByFullName } from '../shared/functions'
import { CryptoListData, ReducedCryptoListData } from './types/Cryptocompare'
const cc = require('cryptocompare')

const handler = async (_req: NextApiRequest, res: NextApiResponse) => {
  let sortedReducedCryptos = {}

  try {
    let cryptoSymbols = {}
    const reducedCryptos: any[] = []
    const coinListResponse = await cc.coinList()

    cryptoSymbols = { ...coinListResponse.Data }

    // Reduce original response to fields we care about in the FE app
    const reduceFields = (cryptoData: CryptoListData): ReducedCryptoListData => {
      return Object.entries(cryptoData).reduce((acc, [key, value]) => {
        switch (key) {
          case 'FullName':
            return { ...acc, ...{ [camelize(key)]: value } }
          case 'Symbol':
            return { ...acc, ...{ [camelize(key)]: value } }
          case 'TotalCoinsMined':
            return { ...acc, ...{ [camelize(key)]: value } }
          default:
            return acc
        }
      }, {}) as ReducedCryptoListData
    }

    // create object of reduced values for cryptos
    for (const entry of Object.entries(cryptoSymbols)) {
      const [key, value] = entry
      const reducedCrypto = reduceFields(value as CryptoListData)

      if (reducedCrypto?.totalCoinsMined > 10000000) {
        reducedCryptos.push(reducedCrypto)
      }
    }

    sortedReducedCryptos = sortObjectByFullName(reducedCryptos)
  } catch (e) {
    console.error(e)
    return res.status(StatusCodes.INTERNAL_SERVER_ERROR).send(e)
  }
  return res.status(StatusCodes.OK).json(sortedReducedCryptos)
}

export default handler
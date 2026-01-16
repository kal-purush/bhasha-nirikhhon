import fs from 'fs'
import IMeta from '../interfaces/IMeta'
import IAntipatternBinaries from '../interfaces/IAntipatternBinaries'
import IAntipatternAbbreviation from '../interfaces/IAntipatternAbbreviation'

const antipatternAbbrevation: IAntipatternAbbreviation = {
  isBreakingSelfDescriptiveness: 'breakingSelfDescriptiveness',
  isForgettingHypermedia: 'forgettingHypermedia',
  isIgnoringCaching: 'ignoringCaching',
  isIgnoringMIMEType: 'ignoringMIMEType',
  isIgnoringStatusCode: 'ignoringStatusCode',
  isMisusingCookies: 'misusingCookies',
  AmorphousURI: 'amorphousURI',
  ContextlessResource: 'contextlessResource',
  CRUDyURI: 'CRUDyURI',
  NonHierarchicalNodes: 'nonHierarchicalNodes',
  PluralisedNodes: 'pluralisedNodes',
}

export const writeTxt = (metas: IMeta[]) => {
  const txtData: IAntipatternBinaries = {}

  metas.forEach((m) => {
    appendData(txtData, m, 'designAntipatterns')
    appendData(txtData, m, 'linguisticAntipatterns')
  })

  let txtString = ''

  const antipatterns = Object.keys(txtData)

  antipatterns.forEach((key) => {
    txtString += `${key}:\n${txtData[key].join(', ')}\n\n`
  })

  fs.writeFileSync('./correlation-data/results.txt', txtString)
}

const appendData = (
  txtData: IAntipatternBinaries,
  meta: IMeta,
  antipatternType: string
): void => {
  Object.keys(meta[antipatternType]).forEach((apKey) => {
    const antipattern = antipatternAbbrevation[apKey]

    if (!txtData[antipattern] /** initializes if doesn't exist */) {
      txtData[antipattern] = []
    }

    const apValue = meta[antipatternType][apKey] ? 1 : 0
    txtData[antipattern].push(apValue)
  })
}
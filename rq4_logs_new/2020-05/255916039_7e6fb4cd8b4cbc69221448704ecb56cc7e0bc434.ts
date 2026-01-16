import fs from 'fs'
import IMeta from '../interfaces/IMeta'
import ISingleCSVData from '../interfaces/ISingleCSVData'
import IAntipatternAbbreviation from '../interfaces/IAntipatternAbbreviation'

// template:

// BSD,FH,IMT,IC,MC,ISC,AMO,CRD,UVU,CRN,NHN,SPN
// 0,0,0,0,0,0,0,0,0,0,0,1
// 0,0,0,0,0,0,0,0,0,0,0,0

// UVU = unversioned uri

const antipatternAbbrevation: IAntipatternAbbreviation = {
  isBreakingSelfDescriptiveness: 'BSD',
  isForgettingHypermedia: 'FH',
  isIgnoringCaching: 'IC',
  isIgnoringMIMEType: 'IMT',
  isIgnoringStatusCode: 'ISC',
  isMisusingCookies: 'MC',
  AmorphousURI: 'AMO',
  ContextlessResource: 'CRN',
  CRUDyURI: 'CRD',
  NonHierarchicalNodes: 'NHN',
  PluralisedNodes: 'SPN',
}

export const writeSingleCSV = (metas: IMeta[]) => {
  const csvData: ISingleCSVData = {}

  metas.forEach((m) => {
    appendData(csvData, m, 'designAntipatterns')
    appendData(csvData, m, 'linguisticAntipatterns')
  })

  let csvString = ''

  const csvHeadings = Object.keys(csvData)

  csvHeadings.forEach((heading, i) => {
    csvString += `${heading}${comma(i, csvHeadings)}`
  })

  csvString += '\n'

  const endpointsAmount = metas.length

  for (let i = 0; i < endpointsAmount; i++) {
    csvHeadings.forEach((heading, headingsIndex) => {
      csvString += `${csvData[heading][i]}${comma(headingsIndex, csvHeadings)}`
    })

    // newline if it isn't last line
    if (i !== endpointsAmount - 1) {
      csvString += '\n'
    }
  }

  fs.writeFileSync('./correlation-data/results.csv', csvString)
}

const comma = (index: number, headings: string[]): string =>
  index === headings.length - 1 ? '' : ','

const appendData = (
  csvData: ISingleCSVData,
  meta: IMeta,
  antipatternType: string
): void => {
  Object.keys(meta[antipatternType]).forEach((apKey) => {
    const antipattern = antipatternAbbrevation[apKey]

    if (!csvData[antipattern] /** initializes if doesn't exist */) {
      csvData[antipattern] = []
    }

    const apValue = meta[antipatternType][apKey] ? 1 : 0
    csvData[antipattern].push(apValue)
  })
}
import fs from 'fs'
import IMeta from '../interfaces/IMeta'
import ICSVData from '../interfaces/ICSVData'

export const writeCSVs = (metas: IMeta[]) => {
  const csvData: ICSVData = {}

  metas.forEach((meta) => {
    appendData(csvData, meta, 'designAntipatterns', 'linguisticAntipatterns')
    appendData(csvData, meta, 'linguisticAntipatterns', 'designAntipatterns')
  })

  Object.keys(csvData).forEach((key) => {
    const data = `${csvData[key].a.join(',')}\n${csvData[key].b.join(',')}`
    fs.writeFileSync(`./correlation-data/csv/${key}.csv`, data)
  })
}

const appendData = (
  csvData: ICSVData,
  meta: IMeta,
  antipatternTypeA: string,
  antipatternTypeB: string
) => {
  Object.keys(meta[antipatternTypeA]).forEach((aKey) => {
    Object.keys(meta[antipatternTypeB]).forEach((bKey) => {
      // initializes if doesn't exist
      if (!csvData[`${aKey}VS${bKey}`]) {
        csvData[`${aKey}VS${bKey}`] = {
          a: [],
          b: [],
        }
      }

      const aAnti = meta[antipatternTypeA][aKey] ? 1 : 0
      const bAnti = meta[antipatternTypeB][bKey] ? 1 : 0

      csvData[`${aKey}VS${bKey}`].a.push(aAnti)
      csvData[`${aKey}VS${bKey}`].b.push(bAnti)
    })
  })
}
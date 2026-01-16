import fs from 'fs'
import IMeta from '../interfaces/IMeta'

interface ITable {
  keyOne: string
  keyTwo: string
  a: number // both
  b: number
  c: number
  d: number // neither
}

export const writeTablesJSON = (metas: IMeta[]) => {
  const tables: ITable[] = []

  metas.forEach((m) => {
    appendData(tables, m, 'linguisticPatterns', 'designAntipatterns')
    appendData(tables, m, 'designPatterns', 'linguisticAntipatterns')
    appendData(tables, m, 'linguisticAntipatterns', 'designAntipatterns')

    // vice versa
    appendData(tables, m, 'designAntipatterns', 'linguisticPatterns')
    appendData(tables, m, 'linguisticAntipatterns', 'designPatterns')
    appendData(tables, m, 'designAntipatterns', 'linguisticAntipatterns')
  })

  fs.writeFileSync(
    './data-files/design-antipatterns/tables.json',
    JSON.stringify(tables)
  )
}

const appendData = (
  tables: ITable[],
  m: IMeta,
  typeOne: string,
  typeTwo: string
) => {
  Object.keys(m[typeOne]).forEach((keyOne) => {
    Object.keys(m[typeTwo]).forEach((keyTwo) => {
      if (!existsTable(tables, keyOne, keyTwo)) {
        tables.push({
          keyOne: keyOne,
          keyTwo: keyTwo,
          a: 0,
          b: 0,
          c: 0,
          d: 0,
        })
      }

      const t = getTables(tables, keyOne, keyTwo)[0]

      if (m[typeOne][keyOne] && m[typeTwo][keyTwo]) {
        t.a++
      }

      if (m[typeOne][keyOne] && !m[typeTwo][keyTwo]) {
        t.b++
      }

      if (!m[typeOne][keyOne] && m[typeTwo][keyTwo]) {
        t.c++
      }

      if (!m[typeOne][keyOne] && !m[typeTwo][keyTwo]) {
        t.d++
      }
    })
  })
}

const getTables = (tables: ITable[], keyOne: string, keyTwo: string) =>
  tables.filter((t) => t.keyOne === keyOne && t.keyTwo === keyTwo)

const existsTable = (tables: ITable[], keyOne: string, keyTwo: string) =>
  getTables(tables, keyOne, keyTwo).length !== 0
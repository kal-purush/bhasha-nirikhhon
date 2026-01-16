import IMeta from '../interfaces/IMeta'
import IRData from '../interfaces/IRData'

export const getRDataCode = (metas: IMeta[]): string => {
  const data: IRData = {
    apis: new Set(),
    antipatternTypes: new Set(),
    apData: {},
  }

  metas.forEach((m) => {
    appendAntipatternData(data, m, 'designAntipatterns')
    appendAntipatternData(data, m, 'linguisticAntipatterns')
  })

  let RVars = '# antipattern count variables:\n\n'
  let RMatrixValues = ''

  let cols = 0

  Object.keys(data['apData']).forEach((key) => {
    RVars += `${key}=${data['apData'][key]}\n`

    RMatrixValues += `${key}, `

    cols++

    if (cols === data.antipatternTypes.size) {
      RVars += '\n'
      RMatrixValues += '\n'
      cols = 0
    }
  })

  // removes last comma with space
  RMatrixValues = RMatrixValues.substring(0, RMatrixValues.length - 3)

  let apCalculation = `#antipatterns

mydata <- matrix(c(${RMatrixValues}), nrow=${data.apis.size},
ncol=${data.antipatternTypes.size},byrow = TRUE)

dimnames(mydata) = list(c(${setToString(data.apis)}),
c(${setToString(data.antipatternTypes)})

chisq.test(mydata)
`

  return `${RVars}${apCalculation}`
}

const appendAntipatternData = (
  data: IRData,
  m: IMeta,
  antipatternType: string
) => {
  data.apis.add(m.api)

  Object.keys(m[antipatternType]).forEach((ap) => {
    data.antipatternTypes.add(ap)

    if (!data['apData'][`${m.api}_${ap}`] /** Create if doesn't exist */) {
      data['apData'][`${m.api}_${ap}`] = 0
    }

    data['apData'][`${m.api}_${ap}`] += m[antipatternType][ap] ? 1 : 0
  })
}

const setToString = (strSet: Set<string>): string =>
  [...strSet].map((str) => `"${str}"`).join(', ')
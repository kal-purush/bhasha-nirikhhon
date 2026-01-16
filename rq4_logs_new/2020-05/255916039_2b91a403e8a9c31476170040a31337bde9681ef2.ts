import fs from 'fs'

interface IResultTable {
  keyOne: string
  keyTwo: string
  a: number // both
  b: number
  c: number
  d: number // neither
  result: number | string
}

export const writeTablesTxt = () => {
  try {
    const tables: IResultTable[] = JSON.parse(
      fs.readFileSync(
        './data-files/design-antipatterns/resultTables.json',
        'utf8'
      )
    )

    const tablesTxt = tables.map((t) => getText(t)).join('\n')

    fs.writeFileSync('./correlation-data/tablesForOverleaf.txt', tablesTxt)

    console.log('Wrote to ./correlation-data/tablesForOverleaf.txt')
  } catch (error) {
    console.log('resultTables.json is probably missing')
  }
}

const getText = (table: IResultTable) =>
  `\\begin{center}
  \\begin{tabular}{| p{35mm} | p{35mm} | p{35mm} | p{35mm} |}
  \\hline
   & & \\textbf{${table.keyTwo}} &
  \\\
  \\hline
  & & yes & no
  \\\
  \\hline
  \\textbf{${table.keyOne}} & yes & ${table.a} & ${table.b}
  \\\
  \\hline
   & no & ${table.c} & ${table.d}
  \\\
  \\hline
  \\multicolumn{4}{|c|}{${fixResult(table.result)}}
  \\\ \\hline
  \\end{tabular}
  \\end{center}
`

const fixResult = (result: string | number) =>
  typeof result === 'string'
    ? 'Result: NA'
    : `Result: Phi Coefficient = ${result.toFixed(4)}, `
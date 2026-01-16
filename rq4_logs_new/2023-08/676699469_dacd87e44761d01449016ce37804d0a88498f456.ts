import { DB } from "@sqlite.org/sqlite-wasm"

interface TableColumn {
    key: string,
    label?: string
}

export interface TableEntity {
    name?: string,
    columns: TableColumn[],
    data: Map<string, string>[]
}

export const getTableEntityCellTable = (db: DB, entityId: number, name?: string): TableEntity => {
    const resultRows: any[] =
        db.exec({ sql: `SELECT row, col, value from tableentity_cell where entity_id=${entityId};`, returnValue: 'resultRows', rowMode: 'array' })
    // sort by row/col:
    resultRows.sort((a, b) => a[0] === b[0] ? (a[1] - b[1]) : (a[0] - b[0]))
    //console.log(`TrfDetailView.useEffect[selected] got ${resultRows.length} sorted entity rows`, resultRows)
    // determine max col:
    const maxCol = resultRows.reduce((prev, cur) => cur[1] > prev ? cur[1] : prev, -1)
    // determine columns:
    const columns: TableColumn[] = []
    for (const rowCell of resultRows) {
        if (rowCell[0] > 0) { break }
        const col = rowCell[1]
        while (columns.length < col) {
            columns.push({ key: columns.length.toString() })
        }
        columns.push({ key: columns.length.toString(), label: rowCell[2] })
    }
    // fill missing columns:
    while (columns.length < maxCol + 1) {
        columns.push({ key: columns.length.toString() })
    }
    // fill data
    const data: Map<string, string>[] = []
    for (const rowCell of resultRows) {
        if (rowCell[0] <= 0) { continue }
        const row = rowCell[0] - 1
        const col = rowCell[1]
        const value = rowCell[2]
        // we keep empty rows (this allows some attacks... e.g. with high values in the db (todo add sanity checks!))
        while (data.length <= row) { data.push(new Map()) }
        const dataRow = data[row]
        dataRow.set(col.toString(), value)
    }
    return { name, columns, data }
}
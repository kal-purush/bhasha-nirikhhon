export function updateRowData() {
    return {
        type: 'ROW_DATA_CHANGED'
    }
}

export function selectRow(row){
    return {
        type: 'ROW_SELECTED',
        selectedRow: {[row.id]:row}
    }
}
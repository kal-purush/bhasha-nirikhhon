export class CollectionService {
  static getData(sheetId: string, sheetName: string) {
    const sheet = SpreadsheetApp.openById(sheetId).getSheetByName(sheetName)
    const rows = sheet.getDataRange().getValues()
    const keys = rows.splice(0, 1)[0]
    return rows.map(function (row) {
      const obj = {}
      row.map(function (item, index) {
        obj[String(keys[index])] = String(item)
      })
      return obj
    })
  }
}
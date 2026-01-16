/**
 * Compile a doclet into a table.
 * @generator
 * @param     {Doclet}   doclet                   The doclet.
 * @param     {String[]} columns                  The column names.
 * @param     {String[]} rows                     The row values.
 * @param     {Object}   options                  Options for the table.
 * @prop      {String}   [options.tableNameColor] The color of the table name.
 * @prop      {String}   [options.tableDescColor] The color of the table description.
 * @returns   {String}                            The resulting table.
 */
function compileTable (doclet, columns, rows, options) {
  const {
    tableNameColor,
    descColor
  } = options

  let columnText = ''
  let columnUnderline = ''

  for (const column of columns) {
    const discrim = column === columns[0] ? '' : '|'

    columnText += discrim + column
    columnUnderline += discrim + '-'.repeat(column.length)
  }

  const returnText = doclet.kind === 'function' || doclet.kind === 'method'
    ? '\r\n<font size=\'+1\'>Returns:</font>\r\n' + doclet.returns.reduce((rA, r) => {
      return rA + (rA.length ? '\r\n\r\n' : '') + '> <font color=\'#f5c842\'>{' + r.type.names.reduce((tA, t) => tA + (tA.length ? '|' : '') + t, '') + '}</font>' + (r.description ? ` - *${r.description}*` : '')
    }, '')
    : ''

  const table =
    `<font size='+2'${tableNameColor ? ` color='${tableNameColor}'` : ''}>${doclet.name}</font>\r\n\r\n<font size='+1' color='${descColor}'>${doclet.classdesc || doclet.description}</font>\r\n\r\n---\r\n${columnText}\r\n${columnUnderline}\r\n${rows}${returnText}`

  return table
}

module.exports = compileTable
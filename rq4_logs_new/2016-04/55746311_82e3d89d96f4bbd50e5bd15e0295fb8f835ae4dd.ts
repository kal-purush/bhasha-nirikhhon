import { Promise } from 'es6-promise';
import MySQL from '../services/mysql';
import Column from '../models/Column';
import util = require('util');

export function parseUpdateColumns(columns: Column[] | any, variablePrefix:string = ''): any {
  let values = {};
  let assignments: any = [];
  let parsed = Column.parseList(columns);
  parsed.forEach((row) => {
    assignments.push(`${row.name} = :${variablePrefix}${row.name}`);
    values[`${variablePrefix}${row.name}`] = row.value;
  });

  return {
    assignments,
    values
  };
}

export default function update(sql: MySQL, tableName: string, set: any | any[], where:any|any[]): Promise<any> {
  let setColumns = parseUpdateColumns(set);
  let whereColumns = parseUpdateColumns(where, 'where_');
  let query = `update ${tableName}\n` +
    `  set ${setColumns.assignments.join(',\n  ')}`;

  if (whereColumns.assignments.length > 0) {
    query += '\n' +
      `  where ${whereColumns.assignments.join(' and\n  ')}`;
  }
  query += ';';

  let args = setColumns.values;
  Object.keys(whereColumns.values).forEach((key) => {
    args[key] = whereColumns.values[key];
  });

  return sql.singleTransaction(query, args);
}
import { Promise } from 'es6-promise';
import MySQL from '../services/mysql';

export default function addForeignKey(
  sql: MySQL,
  tableName: string,
  columns: string[],
  parentTableName: string,
  parentTableColumns: string[],
  keyName?: string,
  referenceDefinitions?: string): Promise<any> {

  if (!keyName) {
    keyName = `${tableName}_${columns.join('_')}_${parentTableName}_${parentTableColumns.join('_')}`;
  }

  let query = `ALTER TABLE ${tableName} ADD CONSTRAINT ${keyName} FOREIGN KEY (${columns.join(', ')})\n` +
    `REFERENCES ${parentTableName}(${parentTableColumns.join(', ')})\n` +
    (referenceDefinitions || '');
  return sql.singleTransaction(query);
}
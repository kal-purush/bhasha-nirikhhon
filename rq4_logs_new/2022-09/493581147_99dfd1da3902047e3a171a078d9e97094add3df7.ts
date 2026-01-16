import { Database } from "better-sqlite3";
import { Command } from "commander";
import { readdirSync, ensureDirSync, readJSONSync } from "fs-extra";
import path from "path";
import prompts from "prompts";
import { PATHS } from "../../paths";
import { arrayToHashmap, sortJSONObjectByKey } from "../../utils/object.utils";
import { getSqliteDb } from "./common";

/***************************************************************************************
 * CLI
 * @example yarn
 *************************************************************************************/

const program = new Command("db:import");
export default program.description("Import strapi data").action(async () => {
  new DBImport().run();
});

/***************************************************************************************
 * Main Methods
 *************************************************************************************/

interface ImportSummary {
  table: string;
  filePath: string;
  summary?: any;
  localData: any[];
  importData: any[];
}
class DBImport {
  private db: Database;

  /**
   *
   **/
  public async run() {
    // setup folders
    const importDir = path.resolve(PATHS.dataDir, "db");
    ensureDirSync(importDir);

    // query list of all tables
    this.db = getSqliteDb();
    const importTableNames = readdirSync(importDir).map((name) => ({
      filePath: path.resolve(importDir, name),
      table: name.replace(".json", ""),
    }));
    // get summary of local and import data
    const data: ImportSummary[] = importTableNames.map(({ filePath, table }) => {
      const importData = readJSONSync(filePath);
      const localData = this.getTableData(table);
      let summary: any;
      summary = this.generateSummary(table, importData, localData);
      return { importData, localData, filePath, table, summary };
    });
    // confirm and process
    const confirmed = await this.confirmImport(data);
    if (confirmed) {
      this.handleImport(data);
    }
  }

  private async confirmImport(data: ImportSummary[]) {
    const summaries = data.map(({ summary }) => summary).filter((v) => v);
    if (summaries.length === 0) {
      console.log("Up to date");
      return false;
    }
    console.table(summaries);
    const { confirmed } = await prompts({ type: "confirm", name: "confirmed", message: "Continue?" });
    return confirmed;
  }

  private handleImport(data: ImportSummary[]) {
    let sequenceData: ImportSummary;
    for (const { table, summary, importData } of data) {
      if (summary) {
        // track sequence data to upload after other inserts
        if (table === "sqlite_sequence") {
          sequenceData = { table, summary, importData } as any;
        } else {
          console.log(table);
          this.truncateTable(table);
          this.insertRows(table, importData, Object.keys(importData[0]));
        }
      }
    }
    if (sequenceData) {
      this.updateSqliteSequence(sequenceData.importData);
    }
  }

  /**
   * Compare local and import data, generating a summary of operations to perform
   * NOTE - tables will be fully deleted and recreated, so output only a rough
   * indication of what the expected outcome will be
   * */
  private generateSummary(table: string, importData: any[], localData: any[]) {
    const ops = { table, INSERT: 0, REPLACE: 0, DELETE: 0, SKIP: 0 };

    let primaryKey = "id";
    if (table === "sqlite_sequence") primaryKey = "name";
    const importHashmap = arrayToHashmap(importData, primaryKey);
    const localHashmap = arrayToHashmap(localData, primaryKey);

    for (const [index, importEntry] of Object.entries(importHashmap)) {
      const localEntry = localHashmap[index];
      // new rows
      if (!localEntry) {
        ops.INSERT++;
      }
      // existing rows (conflict or same)
      else {
        const isSame = this.compareEntries(localEntry, importEntry);
        if (isSame) {
          ops.SKIP++;
        } else {
          ops.REPLACE++;
        }
      }
    }
    for (const [index] of localData.entries()) {
      const importEntry = importData[index];
      if (!importEntry) {
        ops.DELETE++;
      }
    }
    let totalOps = ops.INSERT + ops.DELETE + ops.REPLACE;
    // ignore sequence delete of untracked tables
    if (table === "sqlite_sequence") {
      totalOps = ops.INSERT + ops.REPLACE;
    }
    if (totalOps > 0) {
      return ops;
    }
  }

  private compareEntries(a: any, b: any) {
    if (Object.keys(a).length !== Object.keys(b).length) return false;
    return Object.keys(a).every((key) => a[key] === b[key]);
  }

  /**
   *
   * @param table
   * https://stackoverflow.com/questions/4280041/truncate-a-sqlite-table-if-it-exists
   */
  private truncateTable(table: string) {
    const stmt = this.db.prepare(`DELETE FROM ${table}`);
    return stmt.run();
  }

  private insertRows(table: string, rows: any[], columns: string[]) {
    const columnRefs = columns.map((column) => `'${column}'`).join(", ");
    const valueRefs = columns.map((column) => `@${column}`).join(", ");
    const sql = `INSERT INTO ${table} (${columnRefs}) VALUES (${valueRefs})`;
    const stmt = this.db.prepare(sql);
    const insertMany = this.db.transaction((rows) => {
      for (const row of rows) {
        // console.log(row);
        stmt.run(row);
      }
    });
    return insertMany(rows);
  }

  private updateSqliteSequence(values: { name: string; seq: number }[]) {
    const table = "sqlite_sequence";
    for (const { name, seq } of values) {
      const sql = `UPDATE ${table} SET 'seq' = ${seq} WHERE name = '${name}'`;
      const stmt = this.db.prepare(sql);
      stmt.run();
    }
  }

  private getTableData(table: string) {
    const stmt = this.db.prepare(`SELECT * FROM ${table}`);
    const rows = stmt.all();
    return rows;
  }
}
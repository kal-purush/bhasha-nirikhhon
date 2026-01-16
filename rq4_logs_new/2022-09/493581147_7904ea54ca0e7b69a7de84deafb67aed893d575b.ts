import { Command } from "commander";
import { emptyDirSync, ensureDirSync, writeFileSync } from "fs-extra";
import path from "path";
import { PATHS } from "../../paths";
import { sortJSONObjectByKey } from "../../utils/object.utils";
import { getSqliteDb } from "./common";

/***************************************************************************************
 * CLI
 * @example yarn
 *************************************************************************************/

const program = new Command("db:export");
export default program.description("Export strapi data").action(async () => {
  dbExport();
});

/***************************************************************************************
 * Main Methods
 *************************************************************************************/
/**
 * Call strapi plugin import-export-entries to export data
 * https://market.strapi.io/plugins/strapi-plugin-import-export-entries
 **/
async function dbExport() {
  // setup folders
  const outputDir = path.resolve(PATHS.dataDir, "db");
  ensureDirSync(outputDir);
  emptyDirSync(outputDir);

  // query list of all tables
  const db = getSqliteDb();
  const tablesQuery = db.prepare("SELECT name FROM sqlite_schema WHERE type='table'");
  const allTables: { name: string }[] = tablesQuery.all();

  // filter only to include content-generated tables
  const exportedTables = allTables
    .filter(({ name }) => shouldIncludeTableInExport(name))
    .sort((a, b) => (a.name > b.name ? 1 : -1));

  // export
  for (const { name } of exportedTables) {
    exportTableData(name);
  }

  function exportTableData(name: string) {
    const rowsQuery = db.prepare(`SELECT * from ${name}`);
    const data = rowsQuery.all();
    const exportData = prepareDataForExport(name, data);
    const outputPath = path.resolve(outputDir, `${name}.json`);
    writeFileSync(outputPath, exportData);
  }
}

/** Filter tables to only export content-generated tables */
function shouldIncludeTableInExport(name: string) {
  if (name.startsWith("admin_")) return false;
  if (name.startsWith("i18n_")) return false;
  if (name.startsWith("strapi_")) return false;
  if (name.startsWith("up_")) return false;
  return true;
}

function prepareDataForExport(name: string, data: any[]) {
  // keep sqlite auto-index for exported tables (but ignore for rest)
  // NOTE - we could try reset all sequences to 0, but would break linked tables/files
  if (name === "sqlite_sequence") {
    data = data.filter((entry) => shouldIncludeTableInExport(entry.name));
  }
  // sort all json in alphabetical order
  const sorted = data.map((v) => sortJSONObjectByKey(v));
  // otherwise just convert to formatted string
  return JSON.stringify(sorted, null, 2);
}
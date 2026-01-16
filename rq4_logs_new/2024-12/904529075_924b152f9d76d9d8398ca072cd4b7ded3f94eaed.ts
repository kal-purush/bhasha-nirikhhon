import { SCHEMA_URL, SCHEMA_VERSION, ValidFor, SchemaFile } from 'pathofexile-dat-schema';
import { readDatFile } from './vendor/poedat/dat/dat-file.js';
import { readColumn } from './vendor/poedat/dat/reader.js';
import * as loaders from './vendor/poedat/cli/bundle-loaders.js';
import * as fs from 'fs/promises'
import * as path from 'path'
// import * as run from 'pathofexile-dat/dist.js';
import * as exportTables from './vendor/poedat/cli/export-tables.js';
import * as dat from './vendor/poedat/dat.js';
import * as bundles from './vendor/poedat/bundles.js';

const cacheDir = (patch: string) => path.join(process.cwd(), '/cache', `/${patch}`)

const tblDir = (patch: string) => path.join(cacheDir(patch), 'tables');

export async function singleImport(name: string, patch: string) {
  const cd = cacheDir(patch);

  await fs.mkdir(cd, { recursive: true })

  const loader: loaders.FileLoader = await loaders.FileLoader.create(
    await loaders.CdnBundleLoader.create(path.join(cd, '/cdn'), patch)
  )

  const schema = await (await fetch(SCHEMA_URL)).json();
  if (schema.version !== SCHEMA_VERSION) {
    console.error('Schema has format not compatible with this package. Check for "pathofexile-dat" updates.');
    process.exit(1);
  }
  console.log(`Exporting table "Data/${name}"`);
  const datFile = readDatFile('.datc64', await loader.tryGetFileContents(`Data/${name}.datc64`) ??
    await loader.getFileContents(`Data/${name}.datc64`));
  const headers = exportTables.importHeaders(name, datFile, { patch: patch }, schema);
  const tblPath = tblDir(patch);
  fs.mkdir(tblPath, { recursive: true });
  await fs.writeFile(
    path.join(tblPath, `${name}.json`),
    JSON.stringify(exportTables.exportAllRows(headers, datFile), null, 2)
  );
}

const BaseItemTypes = "BaseItemTypes";
const ArmourTypes = "ArmourTypes";
const WeaponTypes = "WeaponTypes";
const ShieldTypes = "ShieldTypes";
const ComponentAttributeRequirements = "ComponentAttributeRequirements";
const ItemClasses = "ItemClasses";
const AllFiles = [
  BaseItemTypes,
  ArmourTypes,
  WeaponTypes,
  ShieldTypes,
  ComponentAttributeRequirements,
  ItemClasses
];

export async function importPoe1Data(patch: string) {
  return Promise.all(AllFiles.map(async tbl => {
    return singleImport(tbl, patch);
  }))
}

export type ExportedItem = {
  baseItem: {
    Id: string,
    Name: string,
  },
  itemClass: {
    Id: string,
    name: string
  },
  attributeRequirements?: {
    str: number,
    dex: number,
    int: number
  },
  shieldInfo?: {
    block: number
  },
  weaponInfo?: {
    crit: number,
    speed: number,
    aps: number,
    min: number,
    max: number
    rng: number
  },
  armourInfo?: {
    armourMin: number,
    armourMax: number,
    evasionMin: number,
    evasionMax: number,
    energyShieldMin: number,
    energyShieldMax: number
    movementSpeed: number,
    wardMin: number,
    wardMax: number
  }
}

export async function generateExportedItems(patch: string) {
  await importPoe1Data(patch);
  const td = tblDir(patch);
  const bits = JSON.parse(await fs.readFile(path.join(td, 'BaseItemTypes.json'), { encoding: 'utf-8' })) as any[];
  const ics = JSON.parse(await fs.readFile(path.join(td, 'ItemClasses.json'), { encoding: 'utf-8' })) as any[];
  const ars = JSON.parse(await fs.readFile(path.join(td, 'ArmourTypes.json'), { encoding: 'utf-8' })) as any[];
  const wts = JSON.parse(await fs.readFile(path.join(td, 'WeaponTypes.json'), { encoding: 'utf-8' })) as any[];
  const sts = JSON.parse(await fs.readFile(path.join(td, 'ShieldTypes.json'), { encoding: 'utf-8' })) as any[];
  const cars = JSON.parse(await fs.readFile(path.join(td, 'ComponentAttributeRequirements.json'), { encoding: 'utf-8' })) as any[];
  const icMap = Object.fromEntries(ics.map(i => [i["_index"] as number, i]))
  const arMap = Object.fromEntries(ars.map(i => [i.BaseItemTypesKey as number, i]))
  const wtMap = Object.fromEntries(wts.map(i => [i.BaseItemTypesKey as number, i]))
  const stMap = Object.fromEntries(sts.map(i => [i.BaseItemTypesKey as number, i]))
  const carMap = Object.fromEntries(cars.map(i => [i.BaseItemTypesKey as string, i]))
  return bits.map(b => {
    const ic = icMap[b.ItemClassesKey];
    const ar = arMap[b["_index"]];
    const wt = wtMap[b["_index"]];
    const st = stMap[b["_index"]];
    const car = carMap[b.Id];
    return {
      baseItem: {
        Id: b.Id,
        Name: b.Name
      },
      itemClass: {
        Id: ic.Id,
        name: ic.Name
      },
      attributeRequirements: car ? {
        str: car.ReqStr,
        dex: car.ReqDex,
        int: car.ReqInt
      } : undefined,
      shieldInfo: st ? {
        block: st.Block
      } : undefined,
      weaponInfo: wt ? {
        crit: wt.Critical,
        speed: wt.Speed,
        aps: Math.round(((1/wt.Speed) * 1000) * 100) / 100,
        min: wt.DamageMin,
        max: wt.DamageMax,
        rng: wt.RangeMax
      } : undefined,
      armourInfo: ar ? {
        armourMin: ar.ArmourMin,
        armourMax: ar.ArmourMax,
        evasionMin: ar.EvasionMin,
        evasionMax: ar.EvasionMax,
        energyShieldMin: ar.EnergyShieldMin,
        energyShieldMax: ar.EnergyShieldMax,
        movementSpeed: ar.IncreasedMovementSpeed,
        wardMin: ar.WardMin,
        wardMax: ar.WardMax
      } : undefined
    }
  })
}
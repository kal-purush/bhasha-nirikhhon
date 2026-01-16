import { exec as exec_ } from 'child_process';
import { dbConfig } from '../../config';

const exec = require('bluebird').promisify(exec_);
let execOpts = { env : { 'PGPASSWORD' : dbConfig.password} };

async function existDatabase(){
    console.log(`Comprobando si existe la base de datos ${dbConfig.database}`);
    let commandExistDb = `psql -U ${dbConfig.user} -tc "SELECT 1 FROM pg_database WHERE datname = '${dbConfig.database}'"`
    let promise = exec(commandExistDb, execOpts);
    return +await promise;
}

async function createDatabase(){
    console.log(`Creando la base de datos ${dbConfig.database}`)
    let commandCreate = `psql -U ${dbConfig.user} -c "CREATE DATABASE ""${dbConfig.database}"" "`;
    let promise = exec(commandCreate, execOpts);
    return await promise;
}

async function createExtensions() {
    console.log(`Creando las extensiones postgis y citext`);
    let command = `psql -U ${dbConfig.user} -d ${dbConfig.database} -f create-extensions.sql`;
    let promise = exec(command, execOpts);
    return await promise;
}

async function createTables() {
    console.log(`Creando las tablas del schema public`);
    let command = `pg_restore --host localhost --port 5432 --username "${dbConfig.user}" --dbname "${dbConfig.database}" --no-password  --section pre-data --section post-data --schema public --verbose "db-only.backup"`;
    let promise = exec(command, execOpts);
    return await promise;
}

async function createCapasSchema() {
    console.log(`Creando el schema capas`)
    let commandCreate = `psql -U ${dbConfig.user} -d ${dbConfig.database} -c "CREATE SCHEMA IF NOT EXISTS capas"`;
    let promise = exec(commandCreate, execOpts);
    return await promise;
}

async function createEventTriggers() {
    console.log(`Creando EVENT-TRIGGERS`);
    let command = `psql -U ${dbConfig.user} -d ${dbConfig.database} -f create-event-triggers.sql`;
    let promise = exec(command, execOpts);
    return await promise;
}

async function addCapas() {
    console.log(`Creando capas`);
    let command = `pg_restore.exe --host localhost --port 5432 --username "${dbConfig.user}" --dbname "${dbConfig.database}" --no-password  --schema capas --verbose "capas.backup"`;
    let promise = exec(command, execOpts);
    return await promise;
}

async function addDatos(){
    console.log(`Añadiendo datos`)
    let commandDisableTrigger = `psql -U ${dbConfig.user} -c "ALTER TABLE datos DISABLE TRIGGER trg_check_gid_layer_exists"`;
    let commandEnableTrigger = `psql -U ${dbConfig.user} -c "ALTER TABLE datos ENABLE TRIGGER trg_check_gid_layer_exists"`;
    let commandAdd = `pg_restore.exe --host localhost --port 5432 --username "${dbConfig.user}" --dbname "${dbConfig.database}" --no-password  --data-only --table datos --schema public --verbose "tabla-datos.backup"`;
    await exec(commandDisableTrigger, execOpts);
    await exec(commandAdd);
    await exec(commandEnableTrigger, execOpts);
}

async function create(){
    let exist = await existDatabase();
    if(exist !== 1){
        await createDatabase();
        await createExtensions();
        await createTables();
        await createCapasSchema();
        await createEventTriggers();
        await addCapas();
        await addDatos();
    }
}

create();
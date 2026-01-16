import { postgres, tables } from "..";
import emitter, { Events } from "../../events";
import { Schema } from "../../types/impl/database";

const compareSchemas = (currentSchema: Schema, existingSchema: Record<string, any>): boolean => {
    const currentEntries = Object.entries(currentSchema);
    const existingEntries = Object.entries(existingSchema);

    if (currentEntries.length !== existingEntries.length) return false;

    for (const [name, field] of currentEntries) {
        const existingField = existingSchema[name];
        if (!existingField) return false;

        const fieldType = field.type.toLowerCase() === "timestamp" ? "timestamp without time zone" : field.type.toLowerCase() === "decimal" ? "numeric" : field.type.toLowerCase();
        const existingFieldType = existingField.type.toLowerCase();

        const fieldPrimaryKey = field.options?.primaryKey ?? false;
        const existingFieldPrimaryKey = existingField.primaryKey;

        const fieldNotNull = field.options?.notNull === undefined ? false : field.options.notNull;
        const existingFieldNotNull = existingField.notNull;

        const fieldDefault = field.options?.default === undefined ? null : field.options.default === "'[]'" ? "'[]'::jsonb" : field.options.default;
        const existingFieldDefault = existingField.default;

        const fieldCheck = field.options?.check;
        const existingFieldCheck = existingField.check;

        // Compare field types and options
        if (fieldType !== existingFieldType || fieldPrimaryKey !== existingFieldPrimaryKey || fieldNotNull !== existingFieldNotNull || fieldDefault !== existingFieldDefault || fieldCheck !== existingFieldCheck) {
            return false;
        }
    }

    return true;
};

// Function to fetch the existing schema of a table
const getExistingTableSchema = async (tableName: string): Promise<Record<string, any>> => {
    const existingSchema: Record<string, any> = {};

    // Fetch the basic column information
    const query = `
        SELECT 
            column_name,
            data_type,
            column_default,
            is_nullable
        FROM information_schema.columns
        WHERE table_name = '${tableName}';
    `;
    const result = await postgres.query(query);

    for (const row of result.rows) {
        const columnName = row.column_name;
        existingSchema[columnName] = {
            type: row.data_type,
            default: row.column_default,
            notNull: row.is_nullable === "NO",
            primaryKey: false,
        };
    }

    // Fetch primary key information
    const primaryKeyQuery = `
        SELECT 
            kcu.column_name
        FROM 
            information_schema.table_constraints tco
        JOIN 
            information_schema.key_column_usage kcu
        ON 
            kcu.constraint_name = tco.constraint_name
        WHERE 
            tco.table_name = '${tableName}' 
            AND tco.constraint_type = 'PRIMARY KEY';
    `;

    const primaryKeyResult = await postgres.query(primaryKeyQuery);

    for (const row of primaryKeyResult.rows) {
        const columnName = row.column_name;
        if (existingSchema[columnName]) {
            existingSchema[columnName].primaryKey = true;
        }
    }

    return existingSchema;
};

export const createTables = async () => {
    const tableCreationPromises: Promise<void>[] = [];

    for (const { schema, tableName } of tables) {
        const promise = new Promise<void>(async (resolve, reject) => {
            try {
                const tableExistsQuery = `SELECT to_regclass('${tableName}') IS NOT NULL AS exists;`;
                const tableExistsResult = await postgres.query(tableExistsQuery);
                const tableExists = tableExistsResult.rows[0]?.exists;

                if (tableExists) {
                    const existingSchema = await getExistingTableSchema(tableName);

                    if (compareSchemas(schema, existingSchema)) {
                        console.log(`Table ${tableName} already exists with the same schema. Skipping.`);
                        resolve();
                    } else {
                        console.log(`Table ${tableName} exists but schema differs. Updating schema.`);
                        const dropQuery = `DROP TABLE IF EXISTS ${tableName} CASCADE;`;
                        await postgres.query(dropQuery);
                    }
                }

                const fields = Object.entries(schema)
                    .map(([name, field]) => {
                        const fieldType = `${name} ${field.type}`;
                        const constraints: string[] = [];

                        if (field.options?.primaryKey) constraints.push("PRIMARY KEY");
                        if (field.options?.notNull) constraints.push("NOT NULL");
                        if (field.options?.unique) constraints.push("UNIQUE");
                        if (field.options?.default) constraints.push(`DEFAULT ${field.options.default}`);
                        if (field.options?.check) constraints.push(`CHECK (${field.options.check})`);

                        return `${fieldType} ${constraints.join(" ")}`.trim();
                    })
                    .filter(Boolean)
                    .join(",\n");

                const query = `CREATE TABLE IF NOT EXISTS ${tableName} (\n${fields}\n);`;

                await postgres.query(query);
                await emitter.emit(Events.DATABASE_TABLE_CREATE, tableName);

                resolve();
            } catch (e) {
                console.error(`Error creating table ${tableName}:`, e);
                reject(e);
            }
        });

        tableCreationPromises.push(promise);
    }

    await Promise.all(tableCreationPromises);

    const foreignKeyPromises: Promise<void>[] = [];

    for (const { schema, tableName } of tables) {
        const promise = new Promise<void>(async (resolve, reject) => {
            const foreignKeyQueries = Object.entries(schema)
                .filter(([, field]) => field.options?.constraints?.foreignKey)
                .map(([name, field]) => {
                    const { foreignKey } = field.options!.constraints!;
                    if (!foreignKey) return "";

                    const fieldType = `${name} ${field.type}`;
                    const constraints: string[] = [];

                    if (field.options?.primaryKey) constraints.push("PRIMARY KEY");
                    if (field.options?.notNull) constraints.push("NOT NULL");
                    if (field.options?.unique) constraints.push("UNIQUE");
                    if (field.options?.default) constraints.push(`DEFAULT ${field.options.default}`);
                    if (field.options?.check) constraints.push(`CHECK (${field.options.check})`);

                    const onDeleteClause = foreignKey.onDelete ? `ON DELETE ${foreignKey.onDelete}` : "";
                    console.log(`ALTER TABLE ${tableName} ADD COLUMN IF NOT EXISTS ${`${fieldType} ${constraints.join(" ")}`.trim()} CONSTRAINT ${`${tableName}_${foreignKey.table}_fk_${name}`} REFERENCES ${foreignKey.table} (${foreignKey.field}) ${onDeleteClause};`);
                    return `ALTER TABLE ${tableName} ADD COLUMN IF NOT EXISTS ${name} CONSTRAINT ${`${tableName}_${foreignKey.table}_fk_${name}`} REFERENCES ${foreignKey.table} (${foreignKey.field}) ${onDeleteClause};`;
                });

            try {
                for (const query of foreignKeyQueries) {
                    await postgres.query(query);
                }
                resolve();
            } catch (error) {
                console.error(`Error adding foreign key constraints for table ${tableName}:`, error);
                reject(error);
            }
        });

        foreignKeyPromises.push(promise);
    }

    await Promise.all(foreignKeyPromises);
};
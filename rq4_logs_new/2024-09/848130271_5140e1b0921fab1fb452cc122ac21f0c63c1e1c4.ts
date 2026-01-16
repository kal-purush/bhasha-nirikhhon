import type { Knex } from "knex";


export async function up(knex: Knex): Promise<void> {
    return knex.schema.createTable("users", table => {
        table.increments("id").primary();
        table.string("username").notNullable();
        table.string("email").notNullable();
        table.integer("role_id").unsigned();
        table.foreign("role_id").references("roles.id")
            .onDelete("SET NULL")
            .onUpdate("CASCADE");
    });
}


export async function down(knex: Knex): Promise<void> {
    return knex.schema.dropTable("users");
}
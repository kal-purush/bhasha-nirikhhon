// src/database/migrations/roles.ts

export async function up(knex: any) {
    try {
      await knex.schema.createTable('roles', (table: any) => {
        table.increments('id').primary();
        table.string('name').notNullable();
        table.timestamp('created_at').defaultTo(knex.fn.now());
        table.timestamp('updated_at').defaultTo(knex.fn.now());
      });
  
      // Log success message
      console.log('✅ Migration completed: roles table created');
    } catch (error) {
      console.error('❌ Migration failed:', error);
    }
  }
  
  export async function down(knex: any) {
    try {
      await knex.schema.dropTableIfExists('roles');
      console.log('✅ Migration rolled back: roles table dropped');
    } catch (error) {
      console.error('❌ Rollback failed:', error);
    }
  }
  
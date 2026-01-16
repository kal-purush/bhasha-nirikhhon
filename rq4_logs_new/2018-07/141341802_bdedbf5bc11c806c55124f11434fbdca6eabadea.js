exports.up = function(knex, Promise) {
    return knex.schema.createTable('alias', (table) => {
        table.increments();
        table.integer('company_id');
        table.foreign('company_id').references('companies.id')
        table.string('label');
        table.timestamps(true,true);
    })
};

exports.down = function(knex, Promise) {
    return knex.schema.dropTable('alias');
};

exports.up = function(knex, Promise) {
  return Promise.All([
    knex.schema.createTable('users', (table) => {
      table.increments('userid');
      table.text('useraccess');
    }),
    knex.schema.createTable('events', (table) => {
      table.increments('eventid');
      table.text('eventname');
      table.text('eventcategory');
      table.text('eventdate');
      table.integer('eventtime');
      table.integer('eventfee');
    }),
    knex.schema.createTable('users_events', (table) => {
      table.increments('joinid');
      table.integer('userid');
      table.integer('eventid');
    })
  ]);
};

exports.down = function(knex, Promise) {
  return Promise.All([
    knex.schema.dropTable('users'),
    knex.schema.droptable('events'),
    knex.schema.dropTable('users_events')
  ]);
};
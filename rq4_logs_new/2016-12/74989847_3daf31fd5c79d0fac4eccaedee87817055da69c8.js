'use strict'

const Schema = use('Schema')

class UserColumnsTableSchema extends Schema {

  up () {
    this.table('users', (table) => {
      table.text('info')
      table.text('img_url')
    })
  }

  down () {
    this.table('users', (table) => {
      table.dropColumn('info')
      table.dropColumn('img_url')
    })
  }

}

module.exports = UserColumnsTableSchema
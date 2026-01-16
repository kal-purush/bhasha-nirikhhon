import BaseSchema from '@ioc:Adonis/Lucid/Schema'
import { TipoSector } from 'App/Models/Contracts/TipoSector'

export default class extends BaseSchema {
  protected tableName = 'empresas'

  public async up() {
    this.schema.alterTable(this.tableName, (table) => {
      table.increments('id').primary()

      table.string('nombre').notNullable().unique()
      table.text('descripcion').notNullable()
      table.text('enlace_web')
      table.text('enlace_twitter')
      table.text('enlace_linkedin')
      table.string('num_empleados')
      table.string('avatar')
      table.enu('sector', Object.values(TipoSector)).defaultTo(TipoSector.INFORMATICA).notNullable()
      table.date('creacion').notNullable()

      table.integer('user_id').unsigned().references('users.id').index()
      /**
       * Uses timestamptz for PostgreSQL and DATETIME2 for MSSQL
       */
      table.timestamp('created_at', { useTz: true })
      table.timestamp('updated_at', { useTz: true })
    })
  }

  public async down() {
    this.schema.dropTable(this.tableName)
  }
}
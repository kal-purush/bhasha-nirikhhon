import { Injectable } from '@angular/core';
import { SQLite, SQLiteObject } from '@ionic-native/sqlite';

@Injectable()
export class DatabaseProvider {

  constructor(private sqlite: SQLite) { }

  /**
   * getDB
   * Abrir ou criar o banco de dados
   */
  public getDB() {
    return this.sqlite.create({
      name: 'teste_ortopedico.db',
      location: 'default'
    });
  }

  /**
   * createDatabase
   */
  public createDatabase() {
    return this.getDB()
      .then((db: SQLiteObject) => {
        this.createTables(db);
        this.insertDefaultAnatomy(db);
      })
      .catch(e => console.error(e));
  }

  private createTables(db: SQLiteObject){
    db.sqlBatch([
      ['CREATE TABLE IF NOT EXISTS anatomia (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, nome TEXT)'],
      ['CREATE TABLE IF NOT EXISTS teste_ortopedico (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, nome TEXT NOT NULL, descricao TEXT, ativo INTEGER, anatomia_id INTEGER, FOREIGN KEY(anatomia_id) REFERENCES anatomia(id))']
    ])
      .then(() => console.log('Tabelas criadas com sucesso!'))
      .catch((e) => console.error('Erro ao criar as tabelas: ', e));
  }

  /**
   * 
   * @param db 
   * Coluna cervical
   * Coluna toráxica
   * Coluna lombar
   * Ombro
   * Cotovelo
   * Punho
   * Quadril
   * Joelho
   * Tornozelo
   */
  private insertDefaultAnatomy(db: SQLiteObject){
    db.executeSql('SELECT COUNT(id) AS qtd FROM anatomia', {})
      .then((data: any) => {
        if(data.rows.item(0).qtd == 0){
          db.sqlBatch([
            ['INSERT INTO anatomia (nome) VALUES (?)', ['Coluna cervical']],
            ['INSERT INTO anatomia (nome) VALUES (?)', ['Coluna toráxica']],
            ['INSERT INTO anatomia (nome) VALUES (?)', ['Coluna lombar']],
            ['INSERT INTO anatomia (nome) VALUES (?)', ['Ombro']],
            ['INSERT INTO anatomia (nome) VALUES (?)', ['Cotovelo']],
            ['INSERT INTO anatomia (nome) VALUES (?)', ['Punho']],
            ['INSERT INTO anatomia (nome) VALUES (?)', ['Quadril']],
            ['INSERT INTO anatomia (nome) VALUES (?)', ['Joelho']],
            ['INSERT INTO anatomia (nome) VALUES (?)', ['Tornozelo']]
          ])
            .then(() => console.log('Dados inseridos com sucesso!'))
            .catch((e) => console.error('Erro ao incluir dados: ', e));
        }
      })
      .catch((e) => console.error('Erro ao consultar quantidade: ', e));
  }
}
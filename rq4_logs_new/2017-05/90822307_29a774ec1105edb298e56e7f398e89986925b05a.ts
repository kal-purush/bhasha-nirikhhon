import { Injectable } from '@angular/core';
import { SQLite, SQLiteObject } from '@ionic-native/sqlite';
import 'rxjs/add/operator/map';

@Injectable()
export class SqlServiceProvider {

    db: SQLiteObject = null;

    constructor( private sqlite: SQLite, private sqliteObject: SQLiteObject ) {
    }

    openDatabase() {
        this.sqlite.create( {
            name: 'data.db',
            location: 'default'
        } ).then( ( db: SQLiteObject ) => {
            this.db = db;
        } );
    }

    createBookTable() {
        let sql: any = 'create table if not exists book(id integer primary key autoincrement, book_id integer, title string, intro text, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)';
        return this.db.executeSql( sql, [] );
    }

    insert( book_id: number, title: string, intro: string ) {
        let data:any = new Date();
        let sql: any = "insert into book(book_id, title, intro, datetime) values(?,?,?,?)";
        return this.db.executeSql( sql, [ book_id, title, intro, data ] );
    }

    read() {
        let sql = "select * from book";
        return this.db.executeSql( sql, [] ).then( ( res ) => {
            let result: any = [];
            for ( let index = 0; index < res.rows.length; index++ ) {
                result.push( res.rows.item[ index ] );
            }

            return Promise.resolve( result );
        } )
    }
}
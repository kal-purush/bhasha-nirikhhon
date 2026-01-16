'use strict';
/**
 * Before - SQLite testing
 * 
 * Creates a temporary database with some generated test data which will be
 * retrieved when testing the works model.
 * 
 * @author Kashi Samaraweera <kashi@kashis.com.au>
 * @version 0.0.1
 */
module.exports = function(sqliteDB, done) {
    var db = sqliteDB;
    
    db.serialize(
        function() {
            db.run(
                [
                    'CREATE TABLE work (',
                    '   id INTEGER PRIMARY KEY,',
                    '   name VARCHAR(255) NOT NULL,',
                    '   description TEXT,',
                    '   delivery_year TINYINT NOT NULL',
                    '   delivery_quarter TINYINT NOT NULL',
                    ');'
                ].join('')
            );
            db.run(
                [
                    'CREATE TABLE technology (',
                    '   id INTEGER PRIMARY KEY,',
                    '   name VARCHAR(255) NOT NULL,',
                    '   description TEXT,',
                    '   delivery_year TINYINT NOT NULL',
                    '   delivery_quarter TINYINT NOT NULL',
                    ');'
                ].join('')
            );
        }
    );
    
    done();
};
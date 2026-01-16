/**
 * Created by conghuyvn8x on 8/4/2018.
 */
var mysql = require('mysql');

const mysql_pool  = mysql.createPool({
    connectionLimit : 100,
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'vas_dealer'
});


exports.conn = mysql_pool;

exports.select = function(sql,data){
    let result = new Promise(function(resolve, reject){
        mysql_pool.getConnection(function (err, connection) {
            if (err) {
                logger.error(err);
                resolve({errCode:1, data:null});
            }else{
                connection.query(sql, data, function (err, rows) {
                    if (err) {
                        logger.error(err);
                        resolve({errCode:1, data:null});
                    }else{
                        resolve({errCode:0, data:rows});
                        connection.release();
                    }
                });
            }
        });
    });
    return result;
};
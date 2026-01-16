const express = require('express');
const app = express();
const sql = require('mssql');

const config = {
    server: '***',
    user: '***',
    password: '***',
    port: 1433
};

const server = app.listen(8080, function () {
    const host = server.address().address;
    const port = server.address().port;

    console.log(`API listening at http://${host}:${port}`);
});

//  ConnectionPool is required to prevent the multiple connection error
const ConnectionPool = (res, query, txfn) => {
    return new sql.ConnectionPool(config).connect().then(pool => {
        return pool.request().query(query);
    }).then(result => {
        let rows = result.recordset;
        if(typeof txfn === 'function') {
            let txrows = txfn(rows);
            if(txrows !== void 0 && txrows !== null) {
                rows = txrows;
            }
        }
        
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.status(200).send(rows);
        sql.close();
    }).catch(err => {
        res.status(500).send({ message: `${err}`})
        sql.close();
    });
}

app.get('/', function (req, res) {
    ConnectionPool(res, `SELECT GETDATE() AS 'Now'`);
});
app.get('/u/:database/:schema/:table', function (req, res) {
    let database = req.params.database || "AAQI",
        schema = req.params.schema,
        table = req.params.table,
        q = req.query.q || "*",
        f = req.query.f || "1 = 1",
        limit = req.query.limit || 100;   // Query Parameters (i.e. after the ? in the URL)
    
    let query = `SELECT TOP ${limit} ${q} FROM ${database}.${schema}.${table} WHERE ${f}`;
    query = query.replace(/[-;]/g, "");

    ConnectionPool(res, query, (results) => {
        return results;
    });
});


/*
    var ldap = require('ldapjs');

    function authDN(dn, password, cb) {
    var client = ldap.createClient({url: 'ldap://YOUR_SERVER:PORT'});

    client.bind(dn, password, function (err) {
        client.unbind();
        cb(err === null, err);
    });
    }


    function output(res, err) {
    if (res) {
        console.log('success');
    } else {
        console.log('failure');
    }
    }

    // should print "success"
    authDN('cn=user', 'goodpasswd', output);
    // should print "failure"
    authDN('cn=user', 'badpasswd', output);
*/
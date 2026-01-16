const express = require('express');
const app = express();
const porta = 3000;
const mysql = require('mysql');

const fullCycle = 'Full Cycle';

app.get('/', (req, res)=>{
    const connection = getConnection();

    insertPeople(connection, fullCycle);    

    const sql = 'select id, name from people';

    connection.query(sql, function (err, peoples, fields) {
       if (err){throw err;}
       res.send(formatResponse(peoples)); 
    });    

    connection.end();    
});

app.listen(porta, ()=>{
    console.log('rodando na porta ' + porta); 
});

function getConnection() {
    const configDB = {
        host: 'db',
        user: 'root',
        password: 'admin',
        database: 'desafiodb'
    };

    return mysql.createConnection(configDB);
}

function insertPeople(connection, value) {
    const sql = `insert into people(name) values('${value}')`;
    connection.query(sql);
}

function formatResponse(peoples) {
    let lista = [];
    
    peoples.forEach(people => {
        lista.push(`<li>${people.id}: ${people.name}</li>`);
    });

    return `<p><h1>${fullCycle}</h1></p>    
     <ul>${lista}</ul>`
}
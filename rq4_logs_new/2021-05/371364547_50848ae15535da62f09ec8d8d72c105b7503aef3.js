const express = require('express');
const path = require('path');
const PORT = process.env.PORT || 5000;
const buildPath = path.join(__dirname, 'build');
require('dotenv').config();
const { Client } = require('pg')


const app = express();
app.use(express.static(buildPath));


// const pool = new Pool({
//   useR: 'fhxxuconpoftab',
//   host: 'ec2-52-209-134-160.eu-west-1.compute.amazonaws.com',
//   database: 'd920b2nv5njldq',
//   password: '279395f2b64d02b2a0725d67390dd8ac918c571b556a76a6043c5f3939d07a39',
//   port: 5432,
// })

// pool.query('Select * from facts', (err, res) => {
  //   console.log(err, res)
  //   pool.end()
  // })

const QUERY = process.env.SELECT_ALL_QUERY;

const client = new Client({
  connectionString: process.env.CONNECTION_STRING,
  ssl: {
    rejectUnauthorized: false
  }
})
client.connect()



app.get('/sql', (req, res) => {
  client.query(QUERY, (err, results) => {
    if(err){
      return res.send(err)
    }
    else {
      console.log('Connected to database.');
      return (
        res.json(results.rows)
        )
    }
  })
})

// client.end()

// const config = process.env.CONNECTION_STRING;



// const connection = mssql.connect(config, (err) => {
//   if(err) console.log(err);
// });

// app.get('/sql', (req,res) => {
//   connection.query(QUERY, (err, results) => {
//     if(err) {
//       return res.send(err)
//   }
//   else {
//     console.log('Connected to database.');
//       return (res.json({data1: results.recordset})
//       )
//     }
//   })
// })

app.listen(PORT, () => {
  console.log(`server started on port ${PORT}`);
});

module.exports = app;

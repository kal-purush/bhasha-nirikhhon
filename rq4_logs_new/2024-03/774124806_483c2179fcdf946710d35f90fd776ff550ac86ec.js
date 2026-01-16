//rquire dotenv file
require('dotenv').config()
//require pg
const pg = require('pg');
//create express server
const express = require('express');

const app = express();
// const port = process.env.PORT || 3000
app.use(express.json())
app.use(require('morgan')('dev'))

//add db URL
const client = new pg.Client(process.env.DATABASE_URL || `postgres://localhost/${process.env.DB_NAME}`);

//create init aysnc funct
const init = async () => {
  await client.connect();
  console.log('db connected');

let SQL = /*SQL*/ `
  DROP TABLE IF EXISTS employees;
  DROP TABLE IF EXISTS departments;

  CREATE TABLE departments(
    id SERIAL PRIMARY KEY,
    name VARCHAR(50)
  );
  CREATE TABLE employees(
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    department_id INTEGER REFERENCES departments(id) NOT NULL
  )`

  SQL = /*SQL*/  `
  INSERT INTO departments(name) Values('Operations');
  INSERT INTO departments(name) Values('HR');
  INSERT INTO departments(name) Values('Engineering');
  INSERT INTO names(name, department_id) VALUES('David', (SELECT id FROM departments WHERE name = 'Operations'));
  INSERT INTO names(name, department_id) VALUES('Lee', (SELECT id FROM departments WHERE name = 'Engineering'));
  INSERT INTO names(name, department_id) VALUES('Robin', (SELECT id FROM departments WHERE name = 'HR'));
`
//connect to pg client using await
await client.query(SQL);
console.log('tables seeded');
const port = process.env.PORT;
app.listen(port, () => console.log(`listening on port ${port}`))};


app.use((err, req, res, next) =>{
  res.status(500).send(err.message);
})
//invoke init
init();
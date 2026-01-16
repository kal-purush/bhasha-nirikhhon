import { FastifyInstance } from "fastify";

const fastifyPlugin = require("fastify-plugin");
const { Client } = require("pg");

const createUserTable = `
CREATE TABLE IF NOT EXISTS "users" (
    "user_id" INT NOT NULL,
    "username" VARCHAR(100) NOT NULL,
    PRIMARY KEY ("user_id")
);`;

const createJobTable = `
    CREATE TABLE IF NOT EXISTS "jobs" (
	    "job_id" INT NOT NULL,
        "user_id" INT NOT NULL,
        "ref" VARCHAR(100) NOT NULL,
        "title" VARCHAR(100) NOT NULL,
        "superviser" VARCHAR(100) NOT NULL,
        "thisMonth" INT NOT NULL,
        "lastMonth" INT NOT NULL,
        "total" INT NOT NULL,
	    PRIMARY KEY ("job_id")
    );`;

const createEntryTable = `
    CREATE TABLE IF NOT EXISTS "entries" (
	    "entry_id" INT NOT NULL,
	    "job_id" INT NOT NULL,
        "user_id" INT NOT NULL,
	    "created_by" INT NOT NULL,
        "hours" INT NOT NULL,
        "worked" VARCHAR(100) NOT NULL,
        "created" VARCHAR(100) NOT NULL,
        "notes" VARCHAR(1000) NOT NULL,
	    PRIMARY KEY ("entry_id"),
        CONSTRAINT FK_job  
        FOREIGN KEY(job_id)   
        REFERENCES jobs(job_id),
        CONSTRAINT FK_user  
        FOREIGN KEY(user_id)   
        REFERENCES users(user_id)
    );`;

const client = new Client({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
});


async function dbconnector(fastify: FastifyInstance, options: any) {
  try {
    console.log(options);
    await client.connect();
    await client.query(createUserTable);
    console.log("Creating users");
    await client.query(createJobTable);
    console.log("Creating jobs");
    await client.query(createEntryTable);
    console.log("Creating entries");
    console.log("db connected succesfully");
    fastify.decorate("db", { client });
  } catch (err) {
    console.error(err);
  }
}

module.exports = fastifyPlugin(dbconnector);
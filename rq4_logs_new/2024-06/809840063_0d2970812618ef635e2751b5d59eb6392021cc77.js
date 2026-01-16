// crud.js
const pool = require('./db');

async function createEntry(tableName, data) {
  const keys = Object.keys(data).join(', ');
  const values = Object.values(data).map(value => typeof value === 'string' ? `'${value}'` : value).join(', ');
  const queryString = `INSERT INTO ${tableName} (${keys}) VALUES (${values}) RETURNING *`;
  try {
    const result = await pool.query(queryString);
    console.log(`Entry created successfully in ${tableName}:`, result.rows[0]);
    return result.rows[0];
  } catch (error) {
    console.error(`Error creating entry in ${tableName}:`, error);
    throw error;
  }
}

async function readEntries(tableName) {
  const queryString = `SELECT * FROM ${tableName}`;
  try {
    const result = await pool.query(queryString);
    console.log(`Entries fetched from ${tableName}:`, result.rows);
    return result.rows;
  } catch (error) {
    console.error(`Error fetching entries from ${tableName}:`, error);
    throw error;
  }
}

// Implement update and delete operations similarly

module.exports = { createEntry, readEntries };
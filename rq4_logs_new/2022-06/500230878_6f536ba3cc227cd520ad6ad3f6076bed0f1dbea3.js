const fs = require('fs');
require('dotenv').config();
const mongoose = require('mongoose');
const Homes = require('../database/models/models');
async function startServer() {
    await mongoose.connect(process.env.DB_URL);
}

const homes = JSON.parse(
    fs.readFileSync(`${__dirname}/dev-home.json`, 'utf-8')
);

async function importData(homes) {
    try {
        await Homes.create(homes);
        console.log('data loaded successfully');
    } catch (error) {
        console.log('there is an error loading data to MongoDB');
        console.error(error);
    }
}

await startServer();
await importData(homes);
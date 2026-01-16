const mongoose = require('mongoose');
const CONNECTION_STR = 'mongodb://127.0.0.1:27017';
const DATABASE_NAME = 'test1';

const Pet = require("./models/pet");

async function connectDB() {
    try {
        await mongoose.connect(`${CONNECTION_STR}/${DATABASE_NAME}`);
        console.log(`You have be conected to database:${DATABASE_NAME}...`);

        const pets = await Pet.find()
        console.log(pets);
    } catch (error) {
        console.error('Database error connection', err);
    }
};

connectDB();
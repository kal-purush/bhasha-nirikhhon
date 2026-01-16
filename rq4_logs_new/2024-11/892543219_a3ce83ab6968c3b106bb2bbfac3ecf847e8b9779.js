const mongoose = require('mongoose');
const fs = require('fs');
const Question = require('../models/Question'); // Adjust the path based on your folder structure
require('dotenv').config();

const importQuestions = async () => {
  try {
    // Connect to MongoDB
    await mongoose.connect(process.env.MONGO_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true
    });

    console.log('Connected to MongoDB');

    // Read and parse the JSON file
    const data = JSON.parse(fs.readFileSync('./scripts/importQuestions.json', 'utf-8'));

    // Insert the data into the MongoDB collection
    await Question.insertMany(data);
    console.log('Questions successfully added to MongoDB!');

    // Close the connection
    mongoose.connection.close();
  } catch (error) {
    console.error('Error inserting questions:', error);
    mongoose.connection.close();
  }
};

importQuestions();
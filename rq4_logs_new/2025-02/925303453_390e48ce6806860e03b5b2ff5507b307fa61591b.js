const mongoose = require("mongoose");

async function connect(connectionString) {
    try {
        await mongoose.connect(connectionString, { useNewUrlParser: true, useUnifiedTopology: true });
        console.log("Connected to MongoDB");
        return mongoose.connection;
    } catch (error) {
        console.error("MongoDB Connection Error:", error);
        throw error;
    }
}

module.exports = { connect };
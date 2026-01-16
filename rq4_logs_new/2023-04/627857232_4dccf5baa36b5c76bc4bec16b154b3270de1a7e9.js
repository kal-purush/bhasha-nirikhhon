const express = require("express");
const { MongoClient } = require("mongodb");
require("dotenv").config();

const connectionString = process.env.MONGODB_CONNECTION_STRING;

// App faster with init

const client = new MongoClient(connectionString, {
  useUnifiedTopology: true,
});

const app = express();

const getPlanets = async (req, res, next) => {
  //   await client.connect(); - Would be required if calling finally on every route
  try {
    const db = client.db("Planets-app");
    const collection = db.collection("planet-data");

    req.collection = collection;

    next();
  } catch (error) {
    console.error("Error connecting to database:", error);
    res.status(500).send("Error connecting to database");
  }
};

app.use(getPlanets);

app.get("/planets/all", async (req, res, next) => {
  try {
    const entries = await req.collection.find({}).toArray();
    res.json({ data: entries });
  } catch (error) {
    console.log("Error getting entries:", error);
    error.message = "Error getting entries";
    next(error);
  } finally {
    // await client.close(); // Could optionally close on every request here
  }
});

app.get("/planets/:name", async (req, res, next) => {
  try {
    const name = req.params.name;

    // Find the document with the specified name
    const entry = await req.collection.findOne({
      name: { $regex: `^${name}$`, $options: "i" },
    });
    if (!entry) {
      res.status(404).send("Entry not found");
    } else {
      res.json({ data: entry });
    }
  } catch (error) {
    console.log("Error getting entry:", error);
    error.message = "Error getting entry";
    next(error);
  }
});

app.use((err, req, res, next) => {
  console.error("Error:", err); // Log the full error object
  const customErrorMessage = err.message || "Internal server error"; // Get the custom error message or use a default message
  res.status(500).send(customErrorMessage); // Send the custom error message to the client
});

app.listen(5000, () => {
  console.log("Server listening on port 5000");
});

process.on("SIGINT", () => {
  client.close().then(() => {
    console.log("MongoDB client disconnected");
    process.exit(0);
  });
});
const express = require("express");
const cors = require("cors");
const port = process.env.PORT || 5000;
const app = express();
const { MongoClient, ServerApiVersion } = require("mongodb");
require("dotenv").config();
// middleware
app.use(cors());
app.use(express.json());

const uri = `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASS}@cluster0.a23wjbh.mongodb.net/?retryWrites=true&w=majority`;

const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  },
});

async function run() {
  try {
    const applicationData = client
      .db("applicationForm")
      .collection("applicantData");
    const pdfData = client.db("applicationForm").collection("Pdf");

    app.post("/applicationData", async (req, res) => {
      const data = req.body;
      console.log(data);
      const result = await applicationData.insertOne(data);
      res.send(result);
    });
  } catch (err) {
    console.error(err);
  } finally {
  }
}
run().catch(console.dir);

// Routes
app.get("/", (req, res) => {
  res.send("application surver is running");
});

app.listen(port, () =>
  console.log(`application form is running on port ${port}`)
);
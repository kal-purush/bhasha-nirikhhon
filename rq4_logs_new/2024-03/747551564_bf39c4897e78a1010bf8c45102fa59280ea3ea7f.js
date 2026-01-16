import express from "express";
import { MongoClient } from "mongodb";
import { ObjectId } from "mongodb";
import cors from "cors";

const app = express();
const PORT = 4000;

const url = "mongodb+srv://jasmineabi6199:arockia~E91@jasmineabi.favdivu.mongodb.net/?retryWrites=true&w=majority&appName=jasmineabi";

const client = new MongoClient(url);

await client.connect();
console.log("Database Connected Successfully");

app.use(express.json())
app.use(cors())

app.get("/", function (req, res) {
  res.send("Hello World!");
});

app.post("/post", async function (req, res) {
  const postMethod = req.body;

  const POSTMETHOD = await client.db("CRUD").collection("DATA").insertMany([postMethod]);
  res.status(200).send(POSTMETHOD);
});


//---------------get method --------------------

app.get("/get", async function (req, res) {
  const GetMethod = await client.db("CRUD").collection("DATA").find({}).toArray();
  res.status(200).send(GetMethod);
});


//---------single data get method -------------------------
app.get("/get/:singleId", async function (req, res) {
  const { singleId } = req.params;

  const SingleIdGetMethod = await client.db("CRUD").collection("DATA").findOne({ _id: new ObjectId(singleId) });
  res.status(200).send(SingleIdGetMethod);
});

//---------------------------------Update method-------------------------------------------------------------
app.put("/update/:singlePut",  async function (req, res) {
  const { singlePut } = req.params;
  const UpdateMethod = req.body;
  const singleId = await client.db("CRUD").collection("DATA").updateOne({ _id: new ObjectId(singlePut) }, { $set: UpdateMethod });

  res.status(200).send(singleId);
});

//----------------------------Delete method -----------------

app.delete("/delete/:deleteMethod", async function (req, res) {
  const { deleteMethod } = req.params;

  const DELETEMETHOD = await client.db("CRUD").collection("DATA").deleteOne({ _id: new ObjectId(deleteMethod) });

  res.status(200).send(DELETEMETHOD);
});

app.listen(PORT, () => {
  console.log("Server connected on port", PORT);
});
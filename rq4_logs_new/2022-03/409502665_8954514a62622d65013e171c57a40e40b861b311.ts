//#region import
import * as http from "http";
import * as https from "https";
import * as fs from "fs";
import * as bodyParser from "body-parser";
import express from "express";
import * as mongodb from "mongodb";
import cors from "cors";
//#endregion

//#region mongoDB
const mongoClient = mongodb.MongoClient;
const CONNECTION_STRING =
  process.env.MONGODB_URI ||
  "mongodb+srv://admin:admin@cluster0.niwz6.mongodb.net/5B?retryWrites=true&w=majority";
// const CONNECTION_STRING = "mongodb://127.0.0.1:27017";
// const CONNECTION_STRING =
//   "mongodb+srv://admin:admin@cluster0.niwz6.mongodb.net/5B?retryWrites=true&w=majority";
const DB_NAME = "recipeBook";
//#endregion

const PORT: number = parseInt(process.env.PORT) || 1337;
const PORTHTTPS: number = 1338;

//chiavi
let privateKey = fs.readFileSync("keys/privateKey.pem", "utf-8");
let certificate = fs.readFileSync("keys/certificate.crt", "utf-8");
const CREDENTIALS = { key: privateKey, cert: certificate };

const app = express();

const server = http.createServer(app);

server.listen(PORT, () => {
  init();
});

const serverHttps = https.createServer(CREDENTIALS, app);
serverHttps.listen(PORTHTTPS, function () {
  console.log(`Il server http è in ascolto sulla porta ${PORT}`);
  console.log(`Il server https è in ascolto sulla porta ${PORTHTTPS}`);
});

let paginaErrore = "";
function init() {
  fs.readFile("./static/error.html", (err, data) => {
    if (!err) paginaErrore = data.toString();
    else paginaErrore = "<h2>Risorsa non trovata</h2>";
  });
}

/*  ******************************************
    //  elenco delle routes middleware
    ****************************************** */

//  1. Log
app.use("/", (req, res, next) => {
  console.log("---> ", req.method + ": " + req.originalUrl);
  next();
});

//  2. Static route
app.use("/", express.static("./static")); //  next fa in automatico

//  3. Route lettura paramentri post
app.use("/", bodyParser.json());
app.use("/", bodyParser.urlencoded({ extended: true }));

//  4. Log dei parametri
app.use("/", (req, res, next) => {
  if (Object.keys(req.query).length > 0) console.log("GET --->", req.query);
  if (Object.keys(req.body).length > 0) console.log("BODY --->", req.body);
  next();
});

//  5. Connessione al DB
app.use("/", (req, res, next) => {
  mongoClient.connect(CONNECTION_STRING, (err, client) => {
    if (err) res.status(503).send("DB connection error");
    else {
      req["client"] = client;
      next();
    }
  });
});

//  6. Middleware cors
const whitelist = [
  "https://raccapaolo-crudserver.herokuapp.com",
  "http://raccapaolo-crudserver.herokuapp.com",
  "http://localhost:1337",
  "https://localhost:1338",
  "http://localhost:4200",
];
const corsOptions = {
  origin: function (origin, callback) {
    if (!origin) return callback(null, true);
    if (whitelist.indexOf(origin) === -1) {
      var msg =
        "The CORS policy for this site does not " +
        "allow access from the specified Origin.";
      return callback(new Error(msg), false);
    } else return callback(null, true);
  },
  credentials: true,
};
app.use("/", cors(corsOptions));

/*  ******************************************
    elenco delle routes di risposta al client
    ****************************************** */

//  middleware di intercettazione dei parametri
let currentCollection: string = "";
let id: string = "";

app.use("/api/:collection/:id?", (req, res, next) => {
  currentCollection = req.params.collection;
  id = req.params.id;
  next();
});

//  lettura delle collezioni presenti nel db
app.get("/api/getCollections", (req, res, next) => {
  let db = req["client"].db(DB_NAME) as mongodb.Db;
  let collections = db
    .listCollections()
    .toArray()
    .then((data) => res.send(data))
    .catch((err) => res.status(503).send("QUERY: Syntax error"))
    .finally(() => req["client"].close());
});

//  listener specifici

app.get("/api/*", (req, res, next) => {
  if (currentCollection) {
    let db = req["client"].db(DB_NAME) as mongodb.Db;
    let collection = db.collection(currentCollection);
    if (!id) {
      collection
        .find(req["query"])
        .toArray()
        .then((data) => res.send(data))
        .catch((err) => res.status(503).send("QUERY: Syntax error"))
        .finally(() => req["client"].close());
    } else {
      const oId = new mongodb.ObjectId(id);
      collection
        .findOne({ _id: oId })
        .then((data) => res.send(data))
        .catch((err) => res.status(503).send("QUERY: Syntax error"))
        .finally(() => req["client"].close());
    }
  } else {
    res.status(400).send("Parametro mancante: collection");
    req["client"].close();
  }
});

app.post("/api/*", (req, res, next) => {
  if (currentCollection) {
    let db = req["client"].db(DB_NAME) as mongodb.Db;
    let collection = db.collection(currentCollection);
    collection
      .insertOne(req["body"])
      .then((data) => res.send(data))
      .catch((err) => res.status(503).send("QUERY: Syntax error"))
      .finally(() => req["client"].close());
  }
});

app.delete("/api/*", (req, res, next) => {
  if (currentCollection) {
    let db = req["client"].db(DB_NAME) as mongodb.Db;
    let collection = db.collection(currentCollection);
    const oId = new mongodb.ObjectId(id);
    collection
      .deleteOne({ _id: oId })
      .then((data) => res.send(data))
      .catch((err) => res.status(503).send("QUERY: Syntax error"))
      .finally(() => req["client"].close());
  }
});

app.patch("/api/*", (req, res, next) => {
  if (currentCollection) {
    let db = req["client"].db(DB_NAME) as mongodb.Db;
    let collection = db.collection(currentCollection);
    const oId = new mongodb.ObjectId(id);
    collection
      .updateOne({ _id: oId }, { $set: req["body"] })
      .then((data) => res.send(data))
      .catch((err) => res.status(503).send("QUERY: Syntax error"))
      .finally(() => req["client"].close());
  }
});

app.put("/api/*", (req, res, next) => {
  if (currentCollection) {
    let db = req["client"].db(DB_NAME) as mongodb.Db;
    let collection = db.collection(currentCollection);
    const oId = new mongodb.ObjectId(id);
    collection
      .replaceOne({ _id: oId }, req["body"])
      .then((data) => res.send(data))
      .catch((err) => res.status(503).send("QUERY: Syntax error"))
      .finally(() => req["client"].close());
  }
});

/*  ******************************************
    default route e route di gestione degli errori
    ****************************************** */
app.use("/", (req, res, next) => {
  res.status(400);
  res.send("Risorsa non trovata");
});

app.use("/", (err, req, res, next) => {
  console.log("**** ERRORE SERVER ***** " + err); //  da correggere
});
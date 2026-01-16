//#region import
import * as http from "http";
import * as fs from "fs";
import * as bodyParser from "body-parser";
import express from "express";
import * as mongodb from "mongodb";
import cors from "cors";
import { Server, Socket } from "socket.io";
import colors from "colors";
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

//#region HTTP SERVER
const PORT: number = parseInt(process.env.PORT) || 1337;

const app = express();
const server = http.createServer(app);
const io = new Server(server);

server.listen(PORT, () => {
  console.log(`Server in ascolto sulla porta: ${PORT}`);
  init();
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
  db.listCollections()
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
app.use("/", (req, res) => {
  res.status(404);
  if (req.originalUrl.startsWith("/api/")) {
    res.send("Risorsa non trovata");
  } else {
    res.send(paginaErrore);
  }
});

app.use((err, req, res, next) => {
  console.log("**** ERRORE SERVER ***** " + err); //  da correggere
});
//#endregion

//#region SOCKET SERVER
let users=[];
io.on("connection", function (clientSocket) {
  let user = {} as { username: string; room: string; socket: Socket };

  // 1) ricezione username
  clientSocket.on("login", function (userInfo) {
    userInfo = JSON.parse(userInfo);
    // controllo se user esiste giÃ
    let item = users.find(function (item) {
      return item.username == userInfo.username;
    });
    if (item != null) {
      clientSocket.emit("loginAck", "NOK");
    } else {
      user.username = userInfo.username;
      user.room = userInfo.room;
      user.socket = clientSocket;
      users.push(user);
      clientSocket.emit("loginAck", "OK");
      log(
        "User " +
          colors.yellow(user.username) +
          " (sockID=" +
          user.socket.id +
          ") connected!"
      );

      //inserisco username nella stanza corretta
      this.join(user.room);
    }
  });

  // 2) ricezione di un messaggio
  clientSocket.on("message", function (msg) {
    log(
      "User " +
        colors.yellow(user.username) +
        " (sockID=" +
        user.socket.id +
        ") sent " +
        colors.green(msg)
    );
    // notifico a tutti i socket (mittente compreso) il messaggio ricevuto
    let response = {
      from: user.username,
      message: msg,
      date: new Date(),
    };

    // gestione spedizione a tutti, mittente compreso
    // io.sockets.emit("message_notify", JSON.stringify(response));
    io.to(user.room).emit("message_notify", JSON.stringify(response));
  });

  // 3) disconnessione dell'utente
  clientSocket.on("disconnect", function () {
    // ritorna -1 se non lo trova
    let index = users.findIndex(function (item) {
      return item.username == user.username;
    });
    users.splice(index, 1);
    log(" User " + user.username + " disconnected!");
  });
});

// stampa i log con data e ora
function log(msg) {
  console.log(
    colors.cyan("[" + new Date().toLocaleTimeString() + "]") + ": " + msg
  );
}

//#endregion
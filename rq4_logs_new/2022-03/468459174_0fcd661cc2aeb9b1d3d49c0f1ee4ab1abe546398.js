const express = require("express");
const mongoose = require("mongoose");
require("dotenv").config();
const app = express();
const port = process.env.PORT || 8080;
const connectTwitterDB = require("./models");
const Users = require("./models/Users");
const Messages = require("./models/Messages");

const cors = require("cors");

app.use((req, res, next) => {
  next();
});
app.use(express.static("./public/"));
app.use(express.json());
app.use(cors());

app
  .route("/users")
  .get(async (req, res) => {
    const users = await Users.find({}, "_id name email");

    //TODO:ask if it is ok this way info
    res.json(users);
  })
  .post(async (req, res) => {
    const user = await Users.create(req.body);
    res.send(user);
  });

app.route("/users/:id").get(async (req, res) => {
  const user = await Users.findById(req.params.id, "_id name email");
  res.json(user);
});

app.route("/users/:id/messages").get(async (req, res) => {
  console.log(req.params.id);
  const messages = await Messages.find({ id_user: req.params.id });
  res.json(messages);
});

app
  .route("/messages")
  .get(async (req, res) => {
    const messages = await Messages.find({}).sort({ createdAt: -1 });
    res.json(messages);
  })
  .post(async (req, res) => {
    const message = await Messages.create(req.body);
    res.send(message);
  });

app.route("/messages/:id").get(async (req, res) => {
  const messages = await Messages.find({ _id: req.params.id });
  res.json(messages);
});

app.route("/*").get((req, res) => {
  res.send(
    "<h1>Requested route is not a valid route please see Documentation</h1>"
  );
});
connectTwitterDB().then(() => {
  app.listen(port, console.log("STARTED LISTENING ON PORT " + port));
});
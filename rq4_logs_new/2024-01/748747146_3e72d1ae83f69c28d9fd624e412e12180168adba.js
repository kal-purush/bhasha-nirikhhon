const bodyparser = require("body-parser");
const express = require("express");
const app = express();
const mongoose = require("mongoose");
require("dotenv").config();
const user_routes = require("./routes/user");
const todo_routes = require("./routes/todo");

app.use(express.json());
app.use(bodyparser.json());
app.use(bodyparser.urlencoded({ extended: false }));
app.use("/api/auth", user_routes);
app.use("/api/todo", todo_routes);

mongoose.connect(process.env.MONGODB_URI).then(() => {
  console.log("connected to database");
  app.listen(process.env.PORT);
});
const express = require("express");
const dotenv = require("dotenv");
const app = express();
const authRoute = require("./routes/auth");
const postRoute = require("./routes/posts");
const mongoose=require('mongoose');

dotenv.config();
mongoose
  .connect(process.env.DB_CONNECT, { useNewUrlParser: true })
  .then(() => console.log("Connected to MongoDB..."))
  .catch(err => console.error("Could not connect to MongoDB..."));


app.use(express.json());
app.use("/api/user", authRoute);
app.use("/api/post", postRoute);

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Listening on port ${port}...`));
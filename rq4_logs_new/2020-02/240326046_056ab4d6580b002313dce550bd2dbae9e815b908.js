const express = require("express");
const teacherHandler = require("./routes/teacherRoute");
const teachersHandler = require("./routes/teachersRoute");
const bodyParser = require("body-parser");
const app = express();

app.use(bodyParser.json());
app.use("/teacher", teacherHandler);

app.use("/teachers", teachersHandler);

app.get("/", (req, res) => {
  res.status(200).send("Welcome");
});

const server = app.listen("8080", () => {
  console.log(`server listening on port ${server.address().port}`);
});
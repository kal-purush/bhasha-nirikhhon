const express = require("express");
const logger = require("morgan");
const cors = require("cors");
const swaggerUi = require("swagger-ui-express");
// const swaggerDocument = require("./swagger.json");
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "./.env") });

const usersRouter = require("./routes/api/users");
const messagesRouter = require("./routes/api/messages");

const app = express();

const formatsLogger = app.get("env") === "development" ? "dev" : "short";

app.use(logger(formatsLogger));
app.use(cors());
app.use(express.json());
// app.use(express.static("public"));

// app.use("/api-docs", swaggerUi.serve, swaggerUi.setup(swaggerDocument));
app.use("/api/users", usersRouter);
app.use("/api/messages", messagesRouter);

// app.use("/link", (req, res) => {
//   res.sendFile(path.join(__dirname, "./public/link.html"));
// });
// app.use("/succes", (req, res) => {
//   res.sendFile(path.join(__dirname, "./public/succes.html"));
// });

app.use((req, res) => {
  res.status(404).json({ message: "Not found" });
});

app.use((err, req, res, next) => {
  const { status = 500, message = "Server error" } = err;
  res.status(status).json({ message });
});

module.exports = app;
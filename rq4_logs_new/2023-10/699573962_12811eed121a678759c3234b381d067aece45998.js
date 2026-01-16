const express = require("express");
const morgan = require("morgan");
const cookieParser = require("cookie-parser");
const authRoutes = require("./routes/auth.routes.js");
const { createRoles } = require("./helpers/initialConfig.js");

const app = express();
createRoles();

app.use(morgan("dev"));
app.use(express.json());
app.use(cookieParser());

app.use("/api", authRoutes);

module.exports = app;
const express = require("express");
const bodyParser = require("body-parser");
const { sequelize } = require("./models");
const authenticationRoutes = require("./routes/authenticationRoutes");
const courseManagementRoutes = require("./routes/courseManagementRoutes");
const trackingRoutes = require("./routes/trackingRoutes");
const errorHandler = require("./middleware/errorHandler");
const setupSwagger = require("./docs/swagger");
require("dotenv").config();

const app = express();

// Middleware
app.use(bodyParser.json());

// Routes
app.use("/auth", authenticationRoutes);
app.use("/courses", courseManagementRoutes);
app.use("/progress", trackingRoutes);

// Swagger Documentation
setupSwagger(app);

// Error Handler Middleware
app.use(errorHandler);

// Database Sync and Server Start
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`Server is running on port ${PORT}`);

  try {
    await sequelize.authenticate();
    console.log("Database connection has been established successfully.");
    await sequelize.sync({ force: true }); // Set to false in production
    console.log("Database synchronized.");
  } catch (error) {
    console.error("Unable to connect to the database:", error);
  }
});
import express from "express";
import mongoose from "mongoose";
import cors from "cors";
import categoryRoutes from "./routes/categoryRoutes.js";
import subCategoryRoutes from "./routes/subCategoryRoutes.js";
import itemRoutes from "./routes/itemRoutes.js";
import dotenv from "dotenv";

const app = express();
dotenv.config();
const PORT = process.env.PORT || 5000;
const URL = process.env.MONGO_URL;

// Middleware
app.use(cors());
app.use(express.json());

// Routes
app.use(categoryRoutes);
app.use(subCategoryRoutes);
app.use(itemRoutes);

// Connect to MongoDB
mongoose
  .connect(URL, {
    dbName: "menu_management",
    useNewUrlParser: true,
    useUnifiedTopology: true,
    // useCreateIndex: true,
  })
  .then(() => {
    console.log("Connected to MongoDB");
    app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
  })
  .catch((err) => console.error("Error connecting to MongoDB:", err));
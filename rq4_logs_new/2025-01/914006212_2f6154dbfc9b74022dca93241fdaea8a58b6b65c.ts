import dotenv from "dotenv";
import express from "express";
import webhookRoutes from "@/routes/webhook";
import { PORT } from "./config/constants";

// Load environment variables from .env file
dotenv.config();

const app = express();

app.use(express.json());
app.use("/webhook", webhookRoutes);

app.listen(PORT, () => {
  console.log(`🚀 Server listening on port ${PORT}`);
});

export default app;
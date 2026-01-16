import express from "express";
import { clerkWebhooks } from "../Controllers/userController.js";

const userRouter = express.Router();

userRouter.post('/webhooks',clerkWebhooks)

export default userRouter
import express from "express";
import cookieParser from "cookie-parser";
import cors from "cors";

const app = express();

app.use(
  cors({
    origin: true,
    credentials: true,
  })
);

app.use(express.json({ limit: "16kb" }));
app.use(express.urlencoded({ extended: true, limit: "16kb" }));
app.use(express.static("public"));
app.use(cookieParser());

// routes

import userRouter from "./routes/user.routes.js";

// routes declaration

app.use("/api/v1/users", userRouter);

// video routes
import videoRouter from "./routes/video.routes.js";

app.use("/api/v1/videos", videoRouter);

// comment router
import commentRouter from "./routes/comment.routes.js";

app.use("/api/v1/comments", commentRouter);

// like router
import likeRouter from "./routes/like.routes.js";

app.use("/api/v1/likes", likeRouter);

import tweetRouter from "./routes/tweet.routes.js";

app.use("/api/v1/tweets", tweetRouter);

import subscriptionRouter from "./routes/subscription.routes.js";

app.use("/api/v1/subscriptions", subscriptionRouter);

import playlistRouter from "./routes/playlist.routes.js";

app.use("/api/v1/playlist", playlistRouter);

import dashboardRouter from "./routes/dashboard.routes.js";

app.use("/api/v1/dashboard", dashboardRouter);

export { app };
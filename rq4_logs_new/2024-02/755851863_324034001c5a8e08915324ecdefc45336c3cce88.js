import express from "express";

const server = express();

server.use("/images", express.static("./public/images"));
server.listen(4000);
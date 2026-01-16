import express from "express";
import UserController from "./controllers/user/user.controller";

/**
 * index.ts
 * entry point for Express server
 * 
 * tutorials followed to set up:
 * To create the project using TypeScript:                            https://developer.okta.com/blog/2018/11/15/node-express-typescript
 * To get an idea of how to structure it (didn't follow it exactly):  https://medium.com/the-andela-way/structuring-an-express-js-api-fc62efa038c5
 */

const app = express();
const port = 6969; // TODO: make this an environment variable `PORT` so it can bind to the port on Heroku

// use controllers to manage different endpoints
app.use("/user", UserController);

// start the server
app.listen(port, () => console.log(`Server started on port ${port}`));
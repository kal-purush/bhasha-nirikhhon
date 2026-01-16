import "dotenv/config";
import "reflect-metadata";
import "./services/passport";
import express, { Application } from "express";
import bodyParser from "body-parser";
import morgan from "morgan";
import { createConnection } from "typeorm";
import routes from "./routes";

const session = require("express-session");
const passport = require("passport");

// connection settings are in the "ormconfig.json" file
createConnection()
  .then(async (connection) => {
    const app: Application = express();

    app.use(morgan("combined"));
    app.use(bodyParser.json());
    app.use(bodyParser.urlencoded({ extended: true }));
    app.set("views", __dirname + "/views");
    app.set("view engine", "ejs");
    app.use(
      session({
        secret: process.env.SECRET,
        resave: false,
        saveUninitialized: true,
      })
    );
    app.use(passport.initialize());
    app.use(passport.session());
    app.use(express.urlencoded({ extended: false }));
    app.use(routes);
    app.listen(process.env.PORT, () => {
      console.log(`server is running on http://localhost:${process.env.PORT}/`);
    });
  })
  .catch((error) => console.log("Error: ", error));
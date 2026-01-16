import express from "express";
import cors from "cors";
import fs from "fs";
import path from "path";
import { getMockWords } from "../shared/utils/helpers";

const app: express.Express = express();

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.get("/test", (_req, res) => {
  fs.readFile(
    path.resolve(__dirname, "../shared/utils/data.json"),
    { encoding: "utf-8" },
    (_err, data) => {
      res.json(getMockWords(JSON.parse(data)));
    }
  );
});

export default app;
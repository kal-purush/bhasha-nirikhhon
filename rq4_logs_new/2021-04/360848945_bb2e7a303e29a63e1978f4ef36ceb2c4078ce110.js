import express from "express";
import mongoose from "mongoose";
import { studentRouter } from "./routes/studentRouter.js";

const con = async () => {
  try {
    await mongoose.connect(
      "mongodb+srv://raccon:Webfenix2212!@bootcampigti.gdpcx.mongodb.net/grades?retryWrites=true&w=majority",
      {
        useNewUrlParser: true,
        useUnifiedTopology: true,
      }
    );
    console.log("Conectado com sucesso.");
  } catch (error) {
    console.log("Erro ao conectar ao banco: " + error);
  }
};

con();

const app = express();

app.use(express.json());
app.use(studentRouter);

app.listen(3000, () => console.log("API START"));
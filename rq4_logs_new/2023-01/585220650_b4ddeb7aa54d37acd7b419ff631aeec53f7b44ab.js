import express from "express";
import cors from "cors";

const app = express();
app.use(cors());

app.get("/sign-up", (req, res) => {
  // Manda como resposta o texto 'Hello World'
  res.send("Botão Sign Up");
});

// Configura o servidor para rodar na porta 4000
app.listen(4000);
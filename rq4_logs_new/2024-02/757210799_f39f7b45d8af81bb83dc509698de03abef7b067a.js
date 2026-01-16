const express = require("express");
const app = express();
const data = require("./data.json");

const PORT = 3000;

app.get("/", (req, res) => {
  res.send("Hello World");
});

app.get("/notes", (req, res) => {
  res.json(data.notes);
});

app.get("/notes/:id", (req, res) => {
  try {
    const id = Number(req.params.id);
    console.log(typeof id);
    const notesFind = data.notes.find((items) => items.id === id);
    console.log(notesFind);
    res.json(notesFind);
  } catch (error) {
    console.error("Connot find data", error);
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Server listening on ${PORT}`);
});
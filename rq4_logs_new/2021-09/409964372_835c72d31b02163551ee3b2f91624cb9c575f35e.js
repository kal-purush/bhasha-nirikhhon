const express = require("express");
const rss2json = require("rss-to-json");
const app = express();
const port = 3000;

app.get("/", async (req, res) => {
  const { url } = req.query;
  if (url) {
    const rss = await rss2json.parse(url).catch((err) => {
      res.send(err.message);
    });
    if (rss) {
      res.send(JSON.stringify(rss, null, 3));
    } else {
      res.send("Invalid url.");
    }
  } else {
    res.send("Empty url.");
  }
});

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
});
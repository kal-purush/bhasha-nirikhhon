import * as express from "express";
import * as path from "path";
import * as fs from "fs";
import { js_beautify } from "js-beautify";

var app = express();
app.set("views", path.join(__dirname, "../views"));
app.set("view engine", "vash");

app.use("/static", express.static(__dirname + "/static"));

app.get("/", (req, res) => {
  let fullCode = fs.readFileSync(path.join(__dirname, "../input.txt")).toString();

  let model = {
    codes: fullCode.split(/\n/g)
      .map(c => js_beautify(c.replace(/^\[[^\]]+\]\s/, ""), { indent_size: 2 }))
  };

  res.render("index", model);
});

app.listen(3000, "0.0.0.0", () => {
  console.log("Started!");
});
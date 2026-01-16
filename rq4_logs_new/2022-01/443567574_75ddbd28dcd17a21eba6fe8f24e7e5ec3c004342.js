const express = require("express");
const bodyParser = require("body-parser");
const date = require(__dirname + "/date.js") // module exports

const app = express();

const items = ["Buy Food", "Cook food", "Eat food"];
const workItems = [];

//  For EJS
app.set('view engine', 'ejs');

app.use(bodyParser.urlencoded({
  extended: true
}));
// keep css / javascript & other file on public folder to run on server i.e localhost:3000
app.use(express.static("public"));

app.get("/", function(req, res) {
// calling a function from date.js
const day = date.getDate();

  res.render("list", {
    listTitle: day,
    NewListitems: items
  });
});

app.post("/", function(req, res) {
  const item = req.body.newItem;

  if (req.body.list === "Work") {    // if list is equal to work then only it works
    workItems.push(item);
    res.redirect("/work");
  } else {
    items.push(item);
    res.redirect("/");
  }

});

app.get("/work", function(req, res) {
  res.render("list", {
    listTitle: "Work list",
    NewListitems: workItems
  });
});

app.get("/about", function(req, res){
  res.render("about");
})


app.listen(3000, function() {
  console.log("server started on port 3000");
});
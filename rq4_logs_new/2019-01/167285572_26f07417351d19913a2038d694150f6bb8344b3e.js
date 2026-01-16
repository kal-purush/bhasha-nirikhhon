require('dotenv').config();
const express = require("express");
const logger = require("morgan");
const PORT = process.env.PORT || 3000;
const app = express();

const bList = require('./bookList.json')
const bItem = require('./bookItem.json')
const pList = require('./productList.json')
const pItem = require('./productItem.json')

app.use(logger("tiny"));

app.set("views", "./src/views");
app.set("view engine", "pug");

app.use("/static", express.static("./public"));

app.get("/", (request, response) => {
  response.render("main")
});

app.get("/docs/books/list", (request, response) => {
  response.render("bookList", {
  	bList 
  }) 
    
});

app.get("/docs/books/item", (request, response) => {
  response.render("booksItem",{
  	bItem
  })
 
});

app.get("/docs/products/list", (request, response) => {
  response.render("productsList",{
  	pList
  })
 
});

app.get("/docs/products/item", (request, response) => {
  response.render("productsItem",{
  	pItem
  })

});

app.listen(PORT, () => {
  console.log(`Running on PORT ${ PORT }`)
});
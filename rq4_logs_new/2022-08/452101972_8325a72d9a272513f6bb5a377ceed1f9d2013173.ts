import express from "express";
import cors from "cors";
import { AddressInfo } from "net";
import { v4 as uuid} from "uuid"
import { arrayProducts, arRay } from "./data";

const app = express();

app.use(express.json());
app.use(cors());

let arrayFilter: any = [];

// Ex1
// endpoint pega as informações
app.get("/test", (req, res) => {
  res.send("hello world");
});

// Ex3
// endpoint posta as informações
app.post("/post", (req, res) => {
  const name = req.body.name;
  const price = req.body.price;

  arrayProducts.findIndex((indice) => {
    if (indice.name === name) {
      throw "item já existe";
    }
  });

  const newArrayProducts: arRay = {
    id:uuid(),
    name,
    price,
  };
  arrayProducts.push(newArrayProducts);
  res.send(arrayProducts);
});

// Ex4
app.get("/todosprodutos", (req, res) => {
  res.send(arrayProducts);
});

// Ex5
// endpoint modifica as informações
app.put("/put/:id", (req, res) => {
  const newPrice = req.body.price;
  const id = req.params.id;
  arrayProducts.map((index) => {
    if (id === index.id) {
      index.price = newPrice;
    }
    res.send(arrayProducts);
  });
});

// Ex6
// endpoint deleta as informações
app.delete("/del/:id", (req, res) => {
  const id = req.params.id;

 arrayProducts.filter((index)=>{
  if (id === index.id){
    arrayProducts.map((index)=>{    
       index.id !== id
      res.send(index)
  }
})
});

// Ex7
// Ex8
// Ex9

const server = app.listen(process.env.PORT || 3003, () => {
  if (server) {
    const address = server.address() as AddressInfo;
    console.log(`Server is running in http://localhost:${address.port}`);
  } else {
    console.error(`Failure upon starting server.`);
  }
});
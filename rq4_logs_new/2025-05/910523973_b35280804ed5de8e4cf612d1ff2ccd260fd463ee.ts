import express, { Request, Response } from "express";

const addProductRoute = express.Router();

addProductRoute.post("/uploadImage", async (req: Request, res: Response) => {
  const { name, description, price, category, sellerId } = req.body;
});

addProductRoute.get("/testRoute", async (req: Request, res: Response) => {
  res.send("Hello World");
});

export default addProductRoute;
import Product from "../models/productModel.js";
import asyncHandler from "express-async-handler";

// description Fetch all products
// route  get api/products
// access public routes

const getProducts = asyncHandler(async (req, res) => {
  const products = await Product.find({});

  res.json(products);
});

// description Fetch single product
// route  get api/products/:id
// access public routes

const getProductsById = asyncHandler(async (req, res) => {
  const product = await Product.findById(req.params.id);
  if (product) {
    res.json(product);
  } else {
    res.status(404);
    throw new Error("Product not found");
  }
});

export { getProducts, getProductsById };
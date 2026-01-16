const Product = require("../models/product");

const createProduct = async (req, res, next) => {
  try {
    const newProduct = new Product(req.body);
    await newProduct.save();
    res.status(200).json({ msg: "Product Created" });
  } catch (error) {
    next(error);
  }
};

const getProducts = async (req, res, next) => {
  try {
    const products = await Product.find();
    res.status(200).json(products);
  } catch (error) {
    next(error);
  }
};

const getProduct = async (req, res, next) => {
  const { id } = req.params;
  try {
    const productFind = await Product.findById(id);
    res.status(200).json(productFind);
  } catch (error) {
    next(error);
  }
};

const updateProduct = async (req, res, next) => {
  const { id } = req.params;
  try {
    await Product.findByIdAndUpdate(id, req.body);
    res.status(200).json({ msj: "Product Updated" });
  } catch (error) {
    next(error);
  }
};

const deleteProduct = async (req, res, next) => {
  const { id } = req.params;
  try {
    await Product.findByIdAndRemove(id);
    res.status(200).json({ msj: "Product Deleted" });
  } catch (error) {
    next(error);
  }
};

module.exports = {
  createProduct,
  getProducts,
  getProduct,
  updateProduct,
  deleteProduct,
};
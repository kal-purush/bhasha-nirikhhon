const express = require("express");
const router = express.Router();

// Importing the required controllers

const productController = require("../Admin/product.controller");

// Defining routes for CRUD operations on products

// ----------------------------------------------------------------
// Get routes
// ----------------------------------------------------------------

router.get("/admin/product", productController.getAllProducts); // Get all products

router.get("/admin/product/:productId", productController.getAllProducts); // Get product by ID

// ----------------------------------------------------------------
// Post routes
// ----------------------------------------------------------------

router.post("/admin/product/create", productController.addProduct); // Add a new product

module.exports = router;


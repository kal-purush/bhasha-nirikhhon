import express, { Request, Response } from 'express'
import vendorController from '../controllers/vendorController';

const router = express.Router();


// Group endpoints
router.get("/orders/:id", vendorController.getOrders);

router.get("/transactions/:id", vendorController.getTransactions);
router.get("/products/:id",vendorController.getProducts);
// :::::::::::::::::::::::::::::::::::::::::
// single endpoints
router.get("/order/:id", vendorController.getOrder);

router.get("/transaction/:id", vendorController.getTransaction);
router.get("/product/:id", vendorController.getProduct);




export default router;
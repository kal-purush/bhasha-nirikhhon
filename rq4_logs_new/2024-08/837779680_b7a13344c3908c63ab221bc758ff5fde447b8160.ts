import express, { Router } from 'express';
import authMiddleware from '../middlewares/authMiddleware';
import validateJoiRequest from '../middlewares/validateJoiRequest';
import { createOrderValidation } from '../validators/orderFieldsValidation';
import { createOrder } from '../controllers/ordersController';

const orderRouter: Router = express.Router();

orderRouter.route('/')
  .post(
    authMiddleware,
    validateJoiRequest({ bodySchema: createOrderValidation }),
    createOrder,
  );

export default orderRouter;
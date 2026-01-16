import express from "express";
const router = express.Router();

import {
  createOrder,
  getAllOrders,
  getAllSingleOrders,
  getOrder,
  getSingleOrder,
  updateOrder,
} from "../controls/orderController";

import { authRole, authUser } from "../middlewares/authorization";

router
  .route("/")
  .post(authUser, authRole("admin", "user"), createOrder)
  .get(authUser, authRole("admin", "user"), getAllOrders);

router.get(
  "/single-order",
  authUser,
  authRole("admin", "user"),
  getAllSingleOrders
);

router.get(
  "/single-order/:id",
  authUser,
  authRole("admin", "user"),
  getSingleOrder
);

router
  .route("/:id")
  .get(authUser, authRole("admin", "user"), getOrder)
  .patch(authUser, authRole("admin"), updateOrder);

export default router;
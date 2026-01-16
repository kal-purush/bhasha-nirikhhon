import Router from "express";
import passport from "passport";

import {
  CreateAllProducts,
  getAllProductByUserId,
  getAllProducts,
  getProductDetails,
  updateProduct,
  deleteProduct,
} from "../controllers/products";

const router = Router();

router.post(
  "/:_id",
  passport.authenticate("jwt", { session: false }),
  CreateAllProducts
);

router.get(
  "/:userId",
  passport.authenticate("jwt", { session: false }),
  getAllProductByUserId
);

router.get("/:productId", getProductDetails);
router.get("/", getAllProducts);

router.put(
  "/:productId",
  passport.authenticate("jwt", { session: false }),
  updateProduct
);

router.delete(
  "/:productId",
  passport.authenticate("jwt", { session: false }),
  deleteProduct
);

export default router;
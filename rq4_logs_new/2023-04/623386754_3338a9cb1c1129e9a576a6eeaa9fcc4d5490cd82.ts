import express from "express";

const router = express.Router();

import {
  getAllUsers,
  getSingleUser,
  showCurrentUser,
  deleteUser,
  updateUser,
} from "../controls/userController";

import { authUser, authRole } from "../middlewares/authorization";

router.route("/").get(authUser, authRole("admin"), getAllUsers);
router.route("/show-current-user").get(authUser, showCurrentUser);
router
  .route("/:id")
  .get(authUser, authRole("admin"), getSingleUser)
  .patch(authUser, authRole("admin", "user"), updateUser)
  .delete(authUser, authRole("admin", "user"), deleteUser);

export default router;
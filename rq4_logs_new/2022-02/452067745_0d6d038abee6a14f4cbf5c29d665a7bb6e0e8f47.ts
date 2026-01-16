import { Router } from "express";
const router = Router();

import PostsController from "../controllers/PostsController";
const { createPost, getAllUserPosts, getAll } = PostsController;

router.post("/createpost", createPost);
router.get("/myposts", getAllUserPosts);
router.get("/posts", getAll);

export default router;
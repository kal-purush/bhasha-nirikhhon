import { Router } from "express";

const router = Router();

router.get("/", (req, res) => {
  return res.json({ 1: 1 });
});

export default router;
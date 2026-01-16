import express, { Request, Response } from "express";
import { getRepository } from "typeorm";
import { User } from "../entity/auth/auth";

const router = express.Router();

/**
 * @openapi
 * /:
 *   post:
 *     description: signup
 *     responses:
 *       200:
 *         description: created new User
 */
router.post("/", async (req: Request, res: Response) => {
  const userRepository = getRepository(User);
  try {
    const result = await userRepository.save(req.body);
    console.log(result);
  } catch (error) {
    console.log("에러발생");
  }
  res.sendStatus(201);
});

export default router;
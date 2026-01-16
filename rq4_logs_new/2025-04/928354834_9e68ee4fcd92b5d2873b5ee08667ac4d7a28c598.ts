/**
 * @swagger
 * /client/info/{id}:
 *   get:
 *     summary: Get client info by ID
 *     description: Returns client information by ID. Requires authentication. Password is never returned.
 *     tags:
 *       - Client
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *           format: uuid
 *         description: Unique identifier for the client
 *     responses:
 *       200:
 *         description: Client found
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 status:
 *                   type: string
 *                   example: "success"
 *                 data:
 *                   type: object
 *                   properties:
 *                     client:
 *                       type: object
 *                       example:
 *                         id: "string"
 *                         fullName: "Jane Smith"
 *                         email: "client@example.com"
 *                         phone: "1234567890"
 *                         photo: "photo-url"
 *                         createdAt: "2025-04-24T21:35:32.284Z"
 *                         updatedAt: "2025-04-24T21:36:42.285Z"
 *       404:
 *         description: Client not found
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 status:
 *                   type: string
 *                   example: "fail"
 *                 message:
 *                   type: string
 *                   example: "Client not found"
 */
import { Router, Request, Response, NextFunction } from "express";
import prisma from "../../app";
import { isAuthenticated } from "../../middleware/auth";
import logger from "../../utilities/logger";

const router = Router();

// GET /client/info/:id
router.get("/:id", isAuthenticated, async (req: any, res: any, next: NextFunction) => {
  const { id } = req.params;
  try {
    const client = await prisma.client.findUnique({
      where: { id },
      select: {
        id: true,
        fullName: true,
        email: true,
        phone: true,
        photo: true,
        createdAt: true,
        updatedAt: true,
      },
    });
    if (!client) {
      return res.status(404).json({ status: "fail", message: "Client not found" });
    }
    return res.json({ status: "success", data: { client } });
  } catch (error) {
    logger.error("Error fetching client info", { error, id });
    next(error);
  }
});

export default router;
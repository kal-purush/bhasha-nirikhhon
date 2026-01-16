import { Router, Request, Response } from "express";
import prisma from "../../app";
import logger from "../../utilities/logger";

const router = Router();

router.get("/", async (req: Request, res: Response) => {
  const page = parseInt(req.query.page as string) || 1;
  const limit = 10;
  const offset = (page - 1) * limit;

  try {
    /**
     * @swagger
     * /clients:
     *   get:
     *     summary: Retrieve a paginated list of clients
     *     tags: [Clients]
     *     parameters:
     *       - in: query
     *         name: page
     *         schema:
     *           type: integer
     *         description: Page number for pagination
     *     responses:
     *       200:
     *         description: A list of clients with pagination metadata
     *         content:
     *           application/json:
     *             schema:
     *               type: object
     *               properties:
     *                 data:
     *                   type: array
     *                   items:
     *                     $ref: '#/components/schemas/Client'
     *                 meta:
     *                   type: object
     *                   properties:
     *                     totalClients:
     *                       type: integer
     *                       description: Total number of clients
     *                     totalPages:
     *                       type: integer
     *                       description: Total number of pages
     *                     currentPage:
     *                       type: integer
     *                       description: Current page number
     */
    const clients = await prisma.client.findMany({
      skip: offset,
      take: limit,
    });

    const totalClients = await prisma.client.count();
    const totalPages = Math.ceil(totalClients / limit);

    res.status(200).json({
      data: clients,
      meta: {
        totalClients,
        totalPages,
        currentPage: page,
      },
    });
  } catch (error) {
    logger.error('Error fetching clients:', error);
    res.status(500).send("An error occurred while fetching clients");
  }
});

export default router;
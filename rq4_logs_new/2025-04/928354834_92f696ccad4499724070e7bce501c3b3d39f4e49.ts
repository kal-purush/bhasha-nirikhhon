import { Router, Request, Response } from "express";
import prisma from "../../app";
import logger from "../../utilities/logger";

const router = Router();

/**
 * @swagger
 * /client/delete/{id}:
 *   delete:
 *     summary: Soft delete a client by ID
 *     tags: [Clients]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: The client ID
 *     responses:
 *       204:
 *         description: Client marked as deleted successfully
 *       404:
 *         description: Client not found
 *       500:
 *         description: Server error
 */
router.delete('/:id', async (req: Request, res: Response) => {
  const { id } = req.params;
  try {
    await prisma.client.update({ where: { id }, data: { deleted: true } });
    logger.info(`Client deleted: ${id}`);
    res.status(204).send();
  } catch (err) {
    logger.error('Error deleting client', { error: err, id });
    res.status(404).json({ error: 'Client not found' });
  }
});

export default router;
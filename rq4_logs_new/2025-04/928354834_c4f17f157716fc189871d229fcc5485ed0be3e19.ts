/**
 * @swagger
 * /service/update/{id}:
 *   put:
 *     summary: Update an existing service request
 *     description: Update a service request. Different roles have different update permissions.
 *     tags:
 *       - Service
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *           format: uuid
 *         description: Service request ID
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               providerId:
 *                 type: string
 *                 format: uuid
 *                 description: ID of the provider to assign (admin only if already assigned)
 *                 example: "123e4567-e89b-12d3-a456-426614174000"
 *               price:
 *                 type: integer
 *                 minimum: 0
 *                 description: Service price in cents
 *                 example: 15000
 *               serviceRating:
 *                 type: integer
 *                 minimum: 1
 *                 maximum: 5
 *                 description: Rating given by client (only after service completion)
 *                 example: 5
 *               serviceLocation:
 *                 type: string
 *                 description: Location where service is needed
 *                 example: "123 Main St, New York, NY 10001"
 *               done:
 *                 type: boolean
 *                 description: Whether the service is completed
 *                 example: true
 *     responses:
 *       200:
 *         description: Service updated successfully
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
 *                     service:
 *                       $ref: '#/components/schemas/Service'
 *       400:
 *         description: Bad request - validation error
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *             example:
 *               status: "fail"
 *               code: "VALIDATION_ERROR"
 *               message: "Invalid input data"
 *               requestId: "req-123"
 *       401:
 *         description: Unauthorized - not authenticated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *             example:
 *               status: "fail"
 *               code: "AUTHENTICATION_ERROR"
 *               message: "Please log in to access this resource"
 *               requestId: "req-123"
 *       403:
 *         description: Forbidden - not authorized
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *             example:
 *               status: "fail"
 *               code: "AUTHORIZATION_ERROR"
 *               message: "Not authorized to update this service"
 *               requestId: "req-123"
 *       404:
 *         description: Service not found
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *             example:
 *               status: "fail"
 *               code: "NOT_FOUND_ERROR"
 *               message: "Service not found"
 *               requestId: "req-123"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Error'
 *             example:
 *               status: "error"
 *               code: "INTERNAL_SERVER_ERROR"
 *               message: "Something went wrong!"
 *               requestId: "req-123"
 */

import { Router, Request, Response, NextFunction } from "express";
import prisma from "../../app";
import logger from "../../utilities/logger";
import { validateRequest } from "../../middleware/validation";
import { catchAsync } from '../../utilities/catchAsync';
import { isAuthenticated } from "../../middleware/auth";

const router = Router();

// Validation rules for service update
const serviceUpdateRules = {
  providerId: { type: 'string', optional: true },
  price: { type: 'integer', min: 0, optional: true },
  serviceRating: { type: 'integer', min: 1, max: 5, optional: true },
  serviceLocation: { type: 'string', optional: true },
  done: { type: 'boolean', optional: true }
};

router.put("/:id", 
  isAuthenticated,
  validateRequest(serviceUpdateRules), 
  catchAsync(async (req: Request, res: Response, next: NextFunction) => {
    const { id } = req.params;
    const updateData = req.body;

    if (!id) {
      logger.error("Service ID is required");
      return res.status(400).send("Service ID is required");
    }

    try {
      // Check if service exists
      const existingService = await prisma.service.findUnique({ 
        where: { id },
        include: {
          client: true,
          provider: true
        }
      });

      if (!existingService) {
        logger.error("Service not found");
        return res.status(404).send("Service not found");
      }

      // Authorization check
      const userId = (req.user as any).id;
      const userRole = (req.user as any).role;

      const isAuthorized = 
        userRole === 'admin' || 
        existingService.clientId === userId ||
        existingService.providerId === userId;

      if (!isAuthorized) {
        logger.error("Not authorized to update this service");
        return res.status(403).send("Not authorized to update this service");
      }

      // Validate service rating update
      if (updateData.serviceRating !== undefined) {
        if (existingService.clientId !== userId) {
          logger.error("Only the client can update service rating");
          return res.status(403).send("Only the client can update service rating");
        }
        if (!existingService.done) {
          logger.error("Cannot rate service before it's completed");
          return res.status(403).send("Cannot rate service before it's completed");
        }
      }

      // Validate provider assignment
      if (updateData.providerId !== undefined) {
        if (userRole !== 'admin' && existingService.providerId) {
          logger.error("Provider already assigned");
          return res.status(400).send("Provider already assigned");
        }
        // Verify provider exists
        const provider = await prisma.provider.findUnique({
          where: { id: updateData.providerId }
        });
        if (!provider) {
          logger.error("Provider not found");
          return res.status(404).send("Provider not found");
        }
      }

      const updatedService = await prisma.service.update({
        where: { id },
        data: updateData,
        include: {
          client: {
            select: {
              id: true,
              fullName: true,
              email: true,
              phone: true
            }
          },
          provider: {
            select: {
              id: true,
              fullName: true,
              email: true,
              phone: true
            }
          }
        }
      });

      logger.info("Service updated successfully", {
        serviceId: id,
        updatedBy: userId,
        updates: updateData
      });

      return res.json({
        status: 'success',
        data: {
          service: updatedService
        }
      });
    } catch (error) {
      logger.error("Error updating service", { error, serviceId: id });
      next(error);
    }
  })
);

export default router;
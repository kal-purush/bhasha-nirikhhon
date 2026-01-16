import { Request, Response, NextFunction } from "express";
import { prisma } from "../app";
import { ForbiddenError } from "../errors/authorization.error";
import { catchAsync } from "../utilities/catchAsync";

// Middleware to restrict unapproved providers from servicing requests
export const requireProviderApproval = catchAsync(async (req: Request, res: Response, next: NextFunction) => {
  if (!req.user || req.user.role !== "provider") {
    return next(); // Not a provider, skip
  }
  const provider = await prisma.provider.findUnique({ where: { id: req.user.id } });
  if (!provider || !provider.isApproved) {
    throw new ForbiddenError("Your account is pending approval. You cannot accept or service requests until approved by admin.");
  }
  next();
});
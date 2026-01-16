import { Request, Response, NextFunction } from "express";
import APIError from "../utils/APIError";
import User from "../models/User";
import errorHandler from "../utils/errorHandler";

// Middleware to check for admin role
const adminMiddleware = errorHandler(
  (req: Request, res: Response, next: NextFunction) => {
    // Ensure that req.user is defined and is of type User
    const user = (req as any).user as User;

    if (!user) {
      return next(new APIError("Unauthorized: User not found", 401));
    }

    // If the user's role is not 'admin', respond with a 403 Forbidden status
    if (user.role !== "admin") {
      return next(new APIError("Forbidden: Access Denied ", 403));
    }

    next();
  }
);

export default adminMiddleware;
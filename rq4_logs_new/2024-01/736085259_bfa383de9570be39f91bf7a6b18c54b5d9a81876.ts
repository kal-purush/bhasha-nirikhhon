import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';

declare global {
  namespace Express {
    interface Request {
      user?: { _id: string; [key: string]: any }; // Adjust the user object shape according to your payload
    }
  }
}

export function authenticateToken(req: Request, res: Response, next: NextFunction) {
  // Get the token from the cookies
  const token = req.cookies["auth-token"];

  // If no token is found, return an unauthorized error
  if (!token) {
    return res
      .status(401)
      .json({ message: "Access denied. No token provided." });
  }

  try {
    const verified = jwt.verify(
      token,
      process.env.TOKEN_SECRET || "your-default-secret"
    );

    // Check if the verified payload is of the expected format
    if (typeof verified === "object" && verified.hasOwnProperty("_id")) {
      // Safely assert the type because we checked for _id presence
      req.user = verified as { _id: string; [key: string]: any };
      next();
    } else {
      // If the payload doesn't have the expected format, throw an error
      throw new Error("Invalid token payload.");
    }
  } catch (error) {
    res.status(400).json({ message: "Invalid token." });
  }
};
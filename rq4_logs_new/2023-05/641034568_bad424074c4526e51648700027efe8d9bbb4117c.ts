import bcrypt from "bcryptjs";
import { type JwtPayload } from "jsonwebtoken";
import { type Response, type NextFunction, type Request } from "express";
import jwt from "jsonwebtoken";
import User from "../../../database/models/User.js";
import CustomError from "../../CustomError.js";

const loginUser = async (
  req: Request<
    Record<string, unknown>,
    Record<string, unknown>,
    { username: string; password: string }
  >,
  res: Response,
  next: NextFunction
) => {
  const { username, password } = req.body;

  try {
    const user = await User.findOne({ username });

    if (!user) {
      const customError = new CustomError(401, "wrong credentials");

      throw customError;
    }

    if (!(await bcrypt.compare(password, user.password))) {
      const customError = new CustomError(401, "wrong credentials");

      throw customError;
    }

    const tokenPayload: JwtPayload = {
      sub: user._id.toString(),
      name: user.username,
      iat: Math.floor(Date.now() / 1000),
      exp: Math.floor(Date.now() / 1000) + 3600,
    };

    const token = jwt.sign(tokenPayload, process.env.JWT_SECRET!);

    res.status(200).json({ token });
  } catch (error: unknown) {
    next(error);
  }
};

export default loginUser;
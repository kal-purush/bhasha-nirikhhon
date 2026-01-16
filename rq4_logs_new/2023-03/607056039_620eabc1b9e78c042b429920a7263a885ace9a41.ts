
import { Response, NextFunction } from 'express';
import { ApiError } from '@point-hub/express-error-handler';
import { db } from "@src/database/database.js";
import { VerifyTokenUserService } from '@src/modules/auth/services/verify-token.service.js';
import RequestWithUser from '@src/interfaces/RequestWithUser';

async function auth(req: RequestWithUser, res: Response, next: NextFunction) {
  try {
    const authorizationHeader = req.headers.authorization ?? "";

    if (authorizationHeader === "") {
      throw new ApiError(401);
    }

    const verifyTokenUserService = new VerifyTokenUserService(db);
    const result = await verifyTokenUserService.handle(authorizationHeader);

    if(!result) {
      throw new ApiError(401);
    }

    req.user = result;
 
    next();
  } catch (error) {
    next(error);
  }
}

export default auth
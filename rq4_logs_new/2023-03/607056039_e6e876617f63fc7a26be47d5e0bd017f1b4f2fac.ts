
import { Response, NextFunction } from 'express';
import { ApiError } from '@point-hub/express-error-handler';
import { db } from "@src/database/database.js";
import { VerifyTokenUserService } from '@src/modules/auth/services/verify-token.service.js';
import { verify } from "@src/utils/hash.js";
import RequestWithUser from '@src/interfaces/RequestWithUser';
import { QueryInterface } from '@src/database/connection.js';
import { UserRepository } from '@src/modules/auth/repositories/user.repository.js';

async function password(req: RequestWithUser, res: Response, next: NextFunction) {
  try {
    const query: QueryInterface = {
      fields: "",
      filter: { username: req?.user?.username },
      page: 1,
      pageSize: 1,
      sort: "",
    };

    const userRepository = new UserRepository(db);
    const result = (await userRepository.readMany(query)) as any;

    let isVerified = false;
    if (result.pagination.totalDocument === 1) {
      isVerified = await verify(result.data[0].password, req.body.password);
    }

    if (!isVerified) {
      throw new ApiError(401, { message: "You must be confirm your password" });
    }
 
    console.log('done')
    next();
  } catch (error) {
    next(error);
  }
}

export default password
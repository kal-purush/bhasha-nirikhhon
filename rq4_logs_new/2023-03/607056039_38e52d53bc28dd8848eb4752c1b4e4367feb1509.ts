import { ApiError } from '@point-hub/express-error-handler';

import { Response, NextFunction } from 'express';
import RequestWithUser from '@src/interfaces/RequestWithUser';

const permissions = (...allowedPermissions: string[]) => {
  return (req: RequestWithUser, _: Response, next: NextFunction) => {

    console.log(allowedPermissions)
    console.log(req.user?.permissions)
     
    const result = req.user?.permissions?.map(permission => allowedPermissions.includes(permission)).find(val => val === true);
    if(!result) throw new ApiError(403);
     
    next();
  }
}

export default permissions
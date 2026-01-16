import { LOGGER } from 'logger-tracker';
import { SwivRequest } from "../utils/general/general";
import { Response } from 'express';
import * as jwt from 'jsonwebtoken';
import { SERVER_SETTINGS } from '../config';

export function kalturaAuth(req: SwivRequest, res: Response, next: Function): void {

  var token = req.headers['x-access-token'];
  const jwtSecret = SERVER_SETTINGS.jwt.jwtSecret;

  // decode token
  if (!jwtSecret) {
    // if there is no token
    // return an error
    res.status(500).send({
      error: 'jwt configuration failure.'
    });
  }else if (token) {
    jwt.verify(token, jwtSecret, (err: any, decoded: any): void => {
      if (err) {
        res.status(401).send({
          error: 'failed to authenticate token',
          action: err instanceof jwt.TokenExpiredError ? 're-authenticate' : ''
        });
      } else {
        // if everything is good, save to request for use in other routes
        req.kaltura =  {
          partnerId: decoded.partnerId,
          ks: decoded.ks
        };

        next();
      }
    });
  } else {
    // if there is no token
    // return an error
    res.status(403).send({
      error: 'No token provided.',
      reason: ''
    });
  }
}

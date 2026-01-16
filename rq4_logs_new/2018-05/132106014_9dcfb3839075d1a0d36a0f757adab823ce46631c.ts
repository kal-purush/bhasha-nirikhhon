import {CustomError} from "../../model/custom-error";
import * as express from 'express';

export function xhrErrorHandler (err:any, req:express.Request, res:express.Response, next:express.NextFunction) {
  let error:CustomError = new CustomError(err);
  res.status(error.status);
  res.send({message:error.cause,code:error.code});
}
export function globalErrorHandler(err:any, req:express.Request, res:express.Response, next:express.NextFunction) {
  let error:CustomError = new CustomError(err);
  res.status(error.status);
  res.send({message:error.cause,code:error.code});
}

import {Request, Response, NextFunction } from 'express';

// Collective error handler which actions depend on the statuscodes it gets
export function handleErrors(err: Error, req: Request, res: Response, next: NextFunction) {
  switch(res.statusCode) {
    case 500: {
      // Internal errors
      res.send("Something unexpected happened during the request.");
      break;
    }
    case 404: {
      res.send({
        Error : err.message
      })
      break;
    }
    case 400: {
      res.send({
        message : "There seems to be an error with data validation."
      })
      // Validation error, bad syntax
      break;
    }
    default: {
      console.log(err)
      res.send("Something terrible has happened");
      // Something very unexpected, handle as 500
      break;
    }

  }
  
  //res.send({message : "error"})
}
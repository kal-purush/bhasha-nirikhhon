import * as express from 'express';
import { Router } from 'express-serve-static-core';
import { Logger } from 'ts-log-debug';
import * as asyncHandler from 'express-async-handler';
import * as expressListRoutes from 'express-list-routes';
import * as expressPathfinder from 'express-pathfinder';
import * as getRoutes from 'get-routes';

export function StatusRouter(log: Logger): Router {
  log.debug('Initialisiere StatusRouter API.');
  const api = express.Router();
  const app = express();

  api.get('/status', asyncHandler(async (req, res, next) => {
    try {
        // return express.Routes
    } catch (err) {
      log.error(err);
      next(err);
    }
  }));

  return api;
}
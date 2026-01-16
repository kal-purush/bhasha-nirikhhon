import express, { Express, Request, Response } from 'express';
import { addAlice } from '../db/db';

export function endpoints(app: Express) {

  app.get('/', (req: Request, res: Response) => {
    res.send('BACKROW');
    addAlice();
    console.log('add alice from endpoints');
  });

  // add more endpoints here
}
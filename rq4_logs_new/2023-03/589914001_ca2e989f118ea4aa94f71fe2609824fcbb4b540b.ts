import express from 'express';
import airfieldService from '../services/airfieldService';

const router = express.Router();

router.get('/', async (req: express.Request, res: express.Response, next: express.NextFunction) => {
  try {
    const airfields = await airfieldService.getAirfields();
    res.json(airfields);
  } catch (error: unknown) {
    console.log(error);
    next(error);
  }
});

router.get('/:id', async (req: express.Request, res: express.Response, next: express.NextFunction) => {
  const id = Number(req.params.id);
  try {
    const airfield = await airfieldService.getAirfield(id);
    res.json(airfield);
  } catch (error: unknown) {
    next(error);
  }
});

export default router;
import {Request, Response} from 'express';
import knex from '../database/connection';

class CarsController {
  async show(request: Request, response: Response) {
    const allCars = await knex('cars').select('*');

    if(allCars.length > 0) {
      return response.json(allCars);
    }
  }
  
  async create(request: Request, response: Response) {
    const plate = request.body.plate;

    if(plate) {
      const idNewCar = await knex('cars').insert({'plate': plate});

      if(idNewCar.length > 0) {
        return response.json({
          id: idNewCar[0],
          plate
        });
      }
    }
  }
  
  async remove(request: Request, response: Response) {
    const id = request.params.id;

    if(id) {
      const deleted = await knex('cars').where('id', id).del();

      if(deleted) {
        return response.json(deleted);
      }
    }
  }
}

export default CarsController;
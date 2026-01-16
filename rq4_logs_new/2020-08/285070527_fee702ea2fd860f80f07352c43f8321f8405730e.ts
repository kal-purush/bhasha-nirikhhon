import {Request, Response} from 'express';
import Knex from '../database/connection';

class CarParkingController {
  async show(request: Request, response: Response) {    
    const idParking = request.body.id;

    if(idParking) {
      const carsInParking = 
        await Knex('car_parking')
        .where('car_parking.id_parking', idParking)
        .innerJoin('cars', 'cars.id', 'car_parking.id_car')
        .innerJoin('parking', 'parking.id', 'car_parking.id_parking')
        .select('*');

      if(carsInParking.length > 0) {
        return response.json(carsInParking);
      }else {
        return response.json({msg: `not cars in parking id ${idParking}`});
      }
    }
  }
}

export default CarParkingController;
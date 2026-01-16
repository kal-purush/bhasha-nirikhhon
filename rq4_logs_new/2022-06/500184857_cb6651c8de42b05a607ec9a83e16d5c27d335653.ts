import { Request, Response } from 'express';
import rideService from '../services/RideService';
import RideServiceInterface from '../interfaces/RideServiceInterface';

export class RideController {
  private readonly rideService: RideServiceInterface;

  constructor(rideService: RideServiceInterface) {
    this.rideService = rideService;
  }

  getAllRides = async (req: Request, res: Response) => {
    try {
      const rides = await this.rideService.getAll();
      res.status(200).send(rides);
    } catch (error: any) {
      res.status(error.code).send({
        message: error.message,
        errorCode: error.errorCode,
      });
    }
  };

  createRide = async (req: Request, res: Response) => {
    try {
      const rides = await this.rideService.create(req.body);
      res.status(201).send(rides);
    } catch (error: any) {
      res.status(error.code).send({
        message: error.message,
        errorCode: error.errorCode,
      });
    }
  };

  getById = async (req: Request, res: Response) => {
    try {
      const id = parseInt(req.params.id);
      const ride = await this.rideService.getById(id);
      res.status(200).send(ride);
    } catch (error: any) {
      res.status(error.code).send({
        message: error.message,
        errorCode: error.errorCode,
      });
    }
  };
}

export default new RideController(rideService);
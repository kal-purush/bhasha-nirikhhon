import { Request, Response } from 'express';
import { ActivedSport } from '../services/ActivedSports.service';
import { CreateSports } from "../services/CreateSports.service";
import { DeleteSportsService } from '../services/DeleteSports.service';
import { GetAllSportsService } from '../services/GetallSports.service';


export class SportsControllers {


  public async index(request: Request, response: Response) {
    const listSports = new GetAllSportsService();
    const sports = await listSports.execute();
    return response.json(sports);
  }

  public async create(request: Request, response: Response) {
    const { name, category } = request.body;

    const service = new CreateSports();
    const result = await service.execute({ name, category });
    if (result instanceof Error) {
      return response.status(400).json(result.message);
    }
    return response.json(result);
  }

  public async activedSports(request: Request, response: Response) {
    const { id } = request.params;
    const updateSports = new ActivedSport();
    const activedSports = await updateSports.execute(+id);
    return response.json(activedSports);
  }


  public async delete(request: Request, response: Response) {
    const { id } = request.params;
    const deleteSports = new DeleteSportsService();
    await deleteSports.execute(+id);
    response.json('Sports Deleted');

  }
}

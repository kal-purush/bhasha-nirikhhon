import TuitControllerI from "../interfaces/TuitController";
import TuitDaoI from "../interfaces/TuitDao";
import { Request, Response, Express } from "express";
import Tuit from "../models/Tuit"

export default class TuitController implements TuitControllerI {

  private static tuitController: TuitController | null = null;

  private static tuitDao: TuitDaoI;

  public static getInstance = (app: Express, tuitDao: TuitDaoI): TuitController => {

    if (TuitController.tuitController === null) {
      TuitController.tuitController = new TuitController();
    }

    TuitController.tuitDao = tuitDao;
    
    app.get('/api/tuits', TuitController.tuitController.findAllTuits);
    app.get('/api/tuits/:tid', TuitController.tuitController.findTuitById);
    app.get('/api/users/:uid/tuits', TuitController.tuitController.findTuitsByUser);
    app.post('/api/users/:uid/tuits', TuitController.tuitController.createTuit);
    app.delete('/api/tuits/:tid', TuitController.tuitController.deleteTuit);
    app.put('/api/tuits/:tid', TuitController.tuitController.updateTuit);

    return TuitController.tuitController;
  }

  private constructor() {}

  findAllTuits = (req: Request, res: Response) => {
    TuitController.tuitDao
      .findAllTuits()
      .then((tuits: Tuit[]) => res.json(tuits));
  } 

  findTuitById = (req: Request, res: Response)  => {
    TuitController.tuitDao
      .findTuitById(req.params.tid)
      .then((tuit: Tuit) => res.json(tuit));
  }
    
      
  findTuitsByUser = (req: Request, res: Response) => {
    TuitController.tuitDao
      .findTuitsByUser(req.params.uid)
      .then((tuits: Tuit[]) => res.json(tuits));
  }
    

  createTuit = (req: Request, res: Response) => {
    TuitController.tuitDao
      .createTuit(req.body)
      .then((tuit: Tuit) => res.json(tuit));
  }
    

  deleteTuit = (req: Request, res: Response) => {
    TuitController.tuitDao
      .deleteTuit(req.params.tid)
      .then((status: any) => res.json(status));
  }
    

  updateTuit = (req: Request, res: Response) => {
    TuitController.tuitDao
      .updateTuit(req.params.tid, req.body)
      .then((status: any) => res.json(status));
  }
    
}
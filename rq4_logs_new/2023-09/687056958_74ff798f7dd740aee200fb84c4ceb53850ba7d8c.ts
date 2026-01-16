import { Request, Response } from 'express';
import storesService from '../services/storesService'
import userService from '../services/userService'

const list = async (req: Request, res: Response) => {

  const stores = await storesService.list()

  return res.status(200).json(stores)
  
}

const create = async (req: Request, res: Response) => {

  const ownerIdExists = await userService.findOne(req.body.ownerId);

  if (ownerIdExists) {
    const newStore = await storesService.create(req.body)
    return res.status(201).json(newStore)

  }

  return res.status(404).json({ message: 'Inform a valid owner id.'})


}


export default  { list, create };
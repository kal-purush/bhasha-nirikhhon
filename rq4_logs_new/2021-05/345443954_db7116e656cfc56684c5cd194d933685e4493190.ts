import { Response, Request } from 'express'
import User from '../models/User'

export const updateFavorites = async(req: Request, res: Response): Promise<Response> => {
    try{
        const result = await User.updateOne(
           {email: req.body.email} ,
           { $addToSet: {favorites: [req.body.bookId]}}
        )
        return res.status(200).json(result);
    }catch(err){

        return res.status(404).json(err);
    }
}

export const removeFromFavorites = async(req: Request, res: Response): Promise<Response> => {
    try{
        const result = await User.updateOne(
            {email: req.body.email} ,
            { $pull: { favorites : req.body.bookId } as any}
         )
         return res.status(200).json(result);
    }catch(err){

        return res.status(404).json(err);
    }
}
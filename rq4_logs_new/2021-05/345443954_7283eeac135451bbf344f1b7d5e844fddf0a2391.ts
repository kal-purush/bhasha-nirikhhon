import { Response, Request } from 'express'
import Review from '../models/Review'

export const getReviewsForABook = async(req: Request, res: Response): Promise<Response> => {
    try{
        const result = await Review.find({bookId: req.params.id});

        return res.status(200).json(result);
    }catch(err){
        return res.status(404).json(err);
    }
}

export const uploadReview = async(req: Request, res: Response): Promise<Response> => {
    try{

        const review = new Review(req.body);
        const result = await review.save();
        
        return res.status(200).json(result);

    }catch(err){
        return res.status(404).json(err);
    }
}
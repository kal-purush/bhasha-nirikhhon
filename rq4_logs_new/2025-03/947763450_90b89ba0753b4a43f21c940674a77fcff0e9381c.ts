import { injectable } from "inversify";
import BookService from "../services/bookService";
import { Request,Response } from "express";

import ReviewService from "../services/reviewServices";

@injectable()
class ReviewController {
    private reviewService;

    constructor(reviewService: ReviewService) {
        this.reviewService = reviewService
      }

      
      getReviews=async(req:Request,res:Response)=>{
        try{
          const result=await this.reviewService.getReviews(req);
          res.status(result.statusCode).send(result);
        }
        catch(error:any)
        {
          res.status(500).send(error);
        }
      }

      addReviews = async (req: Request, res: Response) => {
        try {
          const result = await this.reviewService.addReviews(req);
          res.status(result.statusCode).send(result);
        } catch (error) {
          res.status(500).send(error);
        }
      };
    
      deleteReviews=async(req:Request,res:Response)=>{
        try{
            const result=await this.reviewService.deleteReviews(req);
            res.status(result.statusCode).send(result);
        }
        catch(error:any)
        {
          res.status(500).send(error);
        }
      }
  }

export default ReviewController;
import { injectable } from "inversify";
import BookService from "../services/bookService";
import { Request,Response } from "express";
import { deleteBook } from "../database/databaseServices";

@injectable()
class BookController
{   
    private bookService
    
    constructor(bookService:BookService){
        this.bookService=bookService
    };

    getBooks=async(req:Request,res:Response)=>{
        try
        {
            const result=await this.bookService.getBooks(req);
            res.status(200).send(result);
        }
        catch(error)
        {
            res.status(500).json({message:`Error in getting books ${error}`});
        }
    }

    getBooksById=async(req:Request,res:Response)=>{
        try
        {
            const result=await this.bookService.getBookById(req);
            res.status(200).send(result);
        }
        catch(error)
        {
            res.status(500).json({message:`Error in getting books ${error}`});
        }
    }

    addBooks=async(req:Request,res:Response)=>{
        try{
            const result=await this.bookService.addBooks(req);
            res.status(200).json({message:result});
        }
        catch(error)
        {
            res.status(500).json({message:`Error in adding books ${error}`});
        }
    }


    updateBooks=async(req:Request,res:Response)=>{
        try{
            const result=await this.bookService.updateBook(req);
            res.status(200).json({message:result});
        }
        catch(error)
        {
            res.status(500).json({message:`Error in adding books ${error}`});
        }
    }

    deleteBook=async(req:Request,res:Response)=>{
        try{
            const result=await this.bookService.deleteBook(req);
            res.status(200).send(result);
        }
        catch(error)
        {
            res.send(500).send(error);
        }
    }
}

export default BookController;
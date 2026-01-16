import { DeleteResult, UpdateResult } from "typeorm";
import { Book } from "../../database/entities/Book";
import bookRepository from "../../database/repository/book.repository";
import { IBookHandler } from "./interface/ibook.handler";

class BookHandler implements IBookHandler {
    async getAllBook(): Promise<Book[]> {
        try {
            const books = await bookRepository.getAllBook();
            return books;
        } catch (error) {
            throw new Error(error);
        }    
    }

    async getBookById(id: number): Promise<Book> {
        try {
            const book = await bookRepository.getBookById(id);
            return book;
        } catch (error) {  
            throw new Error(error);
        }    
    }

    async create(data: Book): Promise<Book> {
        try {
            const newBook = await bookRepository.create(data);
            return newBook;
        } catch (error) {
            throw new Error(error);
        }    
    }
    async update(id: number, data: Book): Promise<UpdateResult> {
      try {
        const updateBook = await bookRepository.update(id, data);
        return updateBook;
      } catch( error) {
        throw new Error(error);
      } 
    }

    async delete(id: number): Promise<DeleteResult> {
        try {
            const deleteBook = await bookRepository.delete(id)
            return deleteBook;
        } catch (error) {
            throw new Error(error)
        }
    }
}

export default new BookHandler();
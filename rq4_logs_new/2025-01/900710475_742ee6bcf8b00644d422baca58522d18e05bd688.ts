import { Meteor } from "meteor/meteor";
import Book from "../types/book";

export async function getBooks(): Promise<Book[]> {
    return await Meteor.call("getBooks", function (_req: any, res: any) {
        if (res.error) {
            console.error("Error getting books:", res.error);
        } else {
            console.log("Books retrieved successfully:", res.result);
            const book: Book = JSON.parse(res.result[0])
            return book;
        }
    });
}

export function getBookById(bookId: string): Promise<Book> {
    Meteor.call("getBookById", bookId, function (_req: any, res: any) {
        if (res.error) {
            console.error("Error getting book with id:", bookId, res.error);
        } else {
            console.log("Book retrieved successfully:", res.result);
            const books: Book[] = JSON.parse(res.result);
            return books
        }
    });
    // Make sure to return a promise
    return new Promise((_resolve, _reject) => {});
}
import { Book } from "./Libro";

export class Library {
    private books: Array<Book> = []

    generarId () {
        const ids = this.books.map(e => e.getBook().id)
    
        if (!ids.length) {
          ids.push(0)
        }
    
        return Math.max(...ids) + 1
      }

    addBook(Book:Book){
        this.books.push(Book)
    }

    getBooks(): Array<Book> {
        return this.books
    }

    getBooksNotTaken(): Array<Book> {
        return this.books.filter(books => books.getDisponible())
    }

    getBooksTaken(): Array<Book> {
        return this.books.filter(books => !books.getDisponible())
    }

    changeBookAvailability(id:number): string {
        return this.books
            .filter(books => books.getBook().id === id)[0]
            .setDisponibility()
    }
}
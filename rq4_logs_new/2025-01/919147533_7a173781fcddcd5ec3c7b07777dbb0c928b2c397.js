class Book {
    // Private properties
    #Title;
    #Author;
    #isAvailable;

    constructor(Title, Author) {
        this.#Title = Title;
        this.#Author = Author;
        this.#isAvailable = true;
    }

    // Public method
    getTitle() {
        return this.#Title;
    }

    // Public method
    getAuthor() {
        return this.#Author;
    }

    // Public method
    isAvailable() {
        return this.#isAvailable;
    }

    // Public method
    checkoutBook() {
        if (this.#isAvailable)
            {
                this.#isAvailable = false
                console.log(`Ви взяли книгу "${this.#Title}" автора ${this.#Author}`);
            }
        else
        {
            console.log(`Вибачте, книга "${this.#Title}" недоступна`);
        }
    }
}

class Library {

    constructor() {
        this.Books = [];
    }
    addBook(Books) {
        this.Books.push(Books);
    }
    checkoutBook(title) {
      const book = this.Books.find(book => book.getTitle() === title);
      if (book) {
          book.checkoutBook();
      } else {
          console.log(`Книга "${title}" не знайдена в бібліотеці`);
      }
    }
      viewAvailableBooks() {
        const availableBooks = [];
        for (let i = 0; i < this.Books.length; i++) {
          if (this.Books[i].isAvailable()) {
            availableBooks.push(`- "${this.Books[i].getTitle()}" автора ${this.Books[i].getAuthor()}`);
          }
        }
        if (availableBooks.length > 0) {
          console.log("Доступні книги:");
          for (let i = 0; i < availableBooks.length; i++) {
            console.log(availableBooks[i]);
          }
        }
        else {
          console.log("Немає доступних книг у бібліотеці.");
        }
      }
    }

    const myLibrary = new Library();
    const book1 = new Book("Фелікс Австрія", "Софія Андрухович");
    const book2 = new Book("Сад Гетсиманський", "Іван Багряний");
    const book3 = new Book("Інститутка", "Марко Вовчок");
    const book4 = new Book("Інтернат", "Сергій Жадан");
    myLibrary.addBook(book1);
    myLibrary.addBook(book2);
    myLibrary.addBook(book3);
    myLibrary.addBook(book4);
    myLibrary.viewAvailableBooks();
    myLibrary.checkoutBook("Інститутка");
    myLibrary.checkoutBook("Безталанна");
    myLibrary.checkoutBook("Фелікс Австрія");
    myLibrary.viewAvailableBooks();
    myLibrary.checkoutBook("Фелікс Австрія");
    myLibrary.checkoutBook("Сад Гетсиманський");
    myLibrary.checkoutBook("Інтернат");
    myLibrary.viewAvailableBooks();
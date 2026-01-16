import bookstore.entity.Book;
import bookstore.service.BookService;

public class MyFirstProject {
    public static void main(String[] args) {

        System.out.println("App started");
        BookService bookService = new BookService();
        Book chrobot = bookService.createBook("Chrobot", 40);
        System.out.println(chrobot.toString());
    }
}
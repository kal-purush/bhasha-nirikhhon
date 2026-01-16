package seu.controller;

import com.owlike.genson.Genson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import seu.domain.Library;
import seu.service.LibraryService;

import java.util.Date;

@Controller
public class LibraryController {

    @Autowired
    LibraryService libraryService;

    Genson genson = new Genson();

    public String getBorrowedBooks() {
        return genson.serialize(libraryService.getAllBookList(MainController.account));
    }

    public int borrowBook(String bookName, int bookId) {
        return libraryService.borrowBook(MainController.account, bookId);
    }

    public int returnBook(int bookId) {
        return libraryService.returnBook(bookId);
    }
}
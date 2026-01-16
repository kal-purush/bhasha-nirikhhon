package ru.otus.spring.rest;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import ru.otus.spring.domain.Author;
import ru.otus.spring.domain.Book;
import ru.otus.spring.domain.Genre;
import ru.otus.spring.rest.dto.BookDto;
import ru.otus.spring.rest.dto.DtoService;
import ru.otus.spring.service.AuthorService;
import ru.otus.spring.service.BookService;
import ru.otus.spring.service.GenreService;

import java.util.List;

@Controller
@RequiredArgsConstructor
public class BookController {

    //    private final BookRepository repository;
    private final BookService bookService;
    private final AuthorService authorService;
    private final GenreService genreService;
    private final DtoService dtoService;

    @GetMapping("/")
    public String listPage(Model model) {

        List<BookDto> bookDtos = dtoService.booksToDto(bookService.getAllBooks());
        model.addAttribute("books", bookDtos);
        return "book_list";
    }

    @GetMapping("/edit")
    public String editPage(@RequestParam("id") long id, Model model) {
        BookDto bookDto = dtoService.bookToDto(bookService.getBookById(id));
        model.addAttribute("book", bookDto);
        return "edit_book";
    }

    @PostMapping("/edit")
    public String saveBook(@RequestParam("id") long id, @ModelAttribute("book") BookDto bookDto) {
        Book book = dtoService.bookDtoToBook(bookDto);
        book.setId(id);
        bookService.saveBook(book);
        return "redirect:/";
    }

    @GetMapping("/add")
    public String addPage() {
        return "add_book";
    }

    @PostMapping("/add")
    public String addBook(@RequestParam("title") String title,
                          @RequestParam("author") String strAuthor,
                          @RequestParam("genre") String strGenre) {

        Genre genre = genreService.getOrCreate(strGenre);
        Author author = authorService.getOrCreate(strAuthor);

        Book book = new Book(0, title, author, genre);
        bookService.saveBook(book);
        return "redirect:/";
    }

    @DeleteMapping("/delete")
    public String deleteBook(@RequestParam("id") long id) {
        System.out.println("delete " + id);
        bookService.deleteBook(id);
        return "redirect:/";
    }

//    @DeleteMapping("/delete")
//    public String deleteBook( @ModelAttribute("bookDto") BookDto bookDto) {
//        System.out.println("delete " + bookDto.getId());
//        bookService.deleteBook(bookDto.getId());
//        return "redirect:/";
//    }
}
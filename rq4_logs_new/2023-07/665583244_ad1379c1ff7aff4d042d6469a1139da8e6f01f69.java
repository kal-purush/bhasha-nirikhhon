package project.vilsoncake.BookOnlineStore.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import project.vilsoncake.BookOnlineStore.entity.BookEntity;
import project.vilsoncake.BookOnlineStore.service.BookService;

@Controller
@RequestMapping("/books")
public class BookController {

    private final BookService bookService;

    public BookController(BookService bookService) {
        this.bookService = bookService;
    }

    @GetMapping
    public String getBook(RedirectAttributes redirectAttributes, @RequestParam String id, Model model) {
        return bookService.getBook(id, redirectAttributes, model);
    }

    @PostMapping
    public String saveNewBook(
            @RequestParam String name,
            @RequestParam String author,
            @RequestParam String publisher,
            @RequestParam Integer yearOfIssue,
            @RequestParam Integer page,
            @RequestParam String language,
            @RequestParam String binding,
            @RequestParam String description,
            @RequestParam String isbn,
            @RequestParam String category,
            @RequestParam Integer startCount,
            @RequestParam MultipartFile avatar,
            Model model
    ) {
        return bookService.addBook(
                new BookEntity(
                        name.trim(),
                        author.trim(),
                        publisher.trim(),
                        yearOfIssue,
                        page,
                        language.trim(),
                        binding.trim(),
                        description.trim(),
                        isbn.trim(),
                        category.trim()
                ), avatar, startCount, model
        );
    }

    @PostMapping("/{id}")
    public String changeBooksCount(@PathVariable("id") String id, RedirectAttributes redirectAttributes, @RequestParam Integer newSetCount, Model model) {
        return bookService.addToSetOfBooks(id, redirectAttributes, newSetCount, model);
    }
}
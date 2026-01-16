package com.example.librarymanagement.controllers;

import com.example.librarymanagement.models.entities.Book;
import com.example.librarymanagement.services.IBookService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/book")
public class BookController {
    private final IBookService iBookService;

    public BookController(IBookService iBookService) {
        this.iBookService = iBookService;
    }


    @GetMapping
    public ResponseEntity<List<Book>> getAllBooks() {
        return new ResponseEntity<>(iBookService.findAll(), HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Book> getBookById(@PathVariable Long id) {
        return new ResponseEntity<>(iBookService.getBookById(id), HttpStatus.OK);
    }

    @GetMapping("/find")
    public ResponseEntity<Book> getBookByTitleAndAuthor(@RequestParam String title, @RequestParam String author) {
        return new ResponseEntity<>(iBookService.getBookByTitleAndAuthor(title, author), HttpStatus.OK);
    }

    @PostMapping("/create")
    public ResponseEntity<Book> createBook(@RequestBody Book book) {
        return new ResponseEntity<>(iBookService.createBook(book), HttpStatus.CREATED);
    }

    @PutMapping("/{id}/update")
    public ResponseEntity<Book> updateBook(@PathVariable Long id, @RequestBody Book book) {
        return new ResponseEntity<>(iBookService.updateBook(id, book), HttpStatus.OK);
    }

    @DeleteMapping("/{id}/delete")
    public ResponseEntity<Book> deleteBook(@PathVariable Long id) {
        iBookService.deleteBook(id);
        return new ResponseEntity<>(HttpStatus.OK);
    }
}
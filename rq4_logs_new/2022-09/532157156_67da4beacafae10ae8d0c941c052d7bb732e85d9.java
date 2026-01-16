package com.bridgelabz.bookstore.controller;

import com.bridgelabz.bookstore.dto.BookDTO;
import com.bridgelabz.bookstore.dto.ResponseDTO;
import com.bridgelabz.bookstore.service.BookService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/book")
public class BookController {
    @Autowired
    private BookService bookService;
    @PostMapping("/add-book")
    public ResponseEntity<ResponseDTO> add(@RequestBody BookDTO bookDTO){
        ResponseDTO responseDTO = new ResponseDTO("Book added successfully",bookService.addBook(bookDTO));
        return ResponseEntity.ok().body(responseDTO);
    }
    @PutMapping("/edit-book/{id}")
    public ResponseEntity<ResponseDTO> edit(@PathVariable long id,@RequestBody BookDTO bookDTO){
        ResponseDTO responseDTO = new ResponseDTO("Book edited successfully",bookService.editBook(bookDTO,id));
        return ResponseEntity.ok().body(responseDTO);
    }

    @DeleteMapping("/delete-book/{id}")
    public ResponseEntity<ResponseDTO> delete(@PathVariable long id){
        ResponseDTO responseDTO = new ResponseDTO("",bookService.deleteBook(id));
        return ResponseEntity.ok().body(responseDTO);
    }
    @GetMapping("/get-book/{id}")
    public ResponseEntity<ResponseDTO> getBook(@PathVariable long id){
        ResponseDTO responseDTO = new ResponseDTO("",bookService.getBookByID(id));
        return ResponseEntity.ok().body(responseDTO);
    }
    @GetMapping("/get-books")
    public ResponseEntity<ResponseDTO> getBooks(){
        ResponseDTO responseDTO = new ResponseDTO("",bookService.getAllBooks());
        return ResponseEntity.ok().body(responseDTO);
    }
    @PostMapping("/change-qty")
    public ResponseEntity<ResponseDTO> changeBookQuantity(@RequestParam long id,@RequestParam int qty){
        ResponseDTO responseDTO = new ResponseDTO("Book quantity changed successfully",bookService.changeBookQty(id,qty));
        return ResponseEntity.ok().body(responseDTO);
    }
    @PostMapping("/change-price")
    public ResponseEntity<ResponseDTO> changeBookPriceController(@RequestParam long id,@RequestParam double price){
        ResponseDTO responseDTO = new ResponseDTO("Book added successfully",bookService.changeBookPrice(id,price));
        return ResponseEntity.ok().body(responseDTO);
    }
}
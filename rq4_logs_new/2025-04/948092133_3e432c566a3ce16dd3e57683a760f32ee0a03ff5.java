package com.myapp.myapp.Controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.myapp.myapp.Service.BorrowingService;
@CrossOrigin(origins = "http://localhost:5173")
@RestController
@RequestMapping("/users")
public class BorrowController {
    @Autowired
    private BorrowingService borrowingService;
    @PostMapping("/{userId}/borrow/{bookId}")
    public String borrowBook(@PathVariable String userId,@PathVariable String bookId){
        borrowingService.addBorrowing(userId, bookId);
        return "Book borrowed successfully";
    }
    @PostMapping("/{userId}/return/{bookId}")
    public String returnBook(@PathVariable String userId,@PathVariable String bookId){
        borrowingService.removeBorrowing(userId, bookId);
        return "Book returned successfully";
    }
}
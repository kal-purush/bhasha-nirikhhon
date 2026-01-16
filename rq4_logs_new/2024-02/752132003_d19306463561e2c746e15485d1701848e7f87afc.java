package com.web.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.web.domain.Yes24BookCrawl;
import com.web.service.BookCrawlService;

@RestController
@RequestMapping("/api")
public class CrawlController {

	@Autowired
	private BookCrawlService bookCrawlService;
	
	@GetMapping("/books")
    public ResponseEntity<List<Yes24BookCrawl>> getBooks() {
        List<Yes24BookCrawl> books = bookCrawlService.getAllBooks();
        return new ResponseEntity<>(books, HttpStatus.OK);
    }
}
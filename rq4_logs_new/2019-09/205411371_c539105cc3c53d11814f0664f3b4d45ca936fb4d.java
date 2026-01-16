package com.angkorsuntrix.demosynctrix.controller;

import com.angkorsuntrix.demosynctrix.domain.Page;
import com.angkorsuntrix.demosynctrix.repository.PageRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;

@Controller
public class PageController {

    @Autowired
    private
    PageRepository repository;

    @GetMapping("/pages")
    public HttpEntity getAll() {
        List<Page> pages = repository.getAll();
        return new ResponseEntity<>(pages, HttpStatus.OK);
    }

    @PostMapping("/pages")
    public HttpEntity create(@RequestBody Page page) {
        final Page createdPage = repository.save(page);
        return new ResponseEntity<>(createdPage, HttpStatus.OK);
    }

    @PutMapping("/pages")
    public HttpEntity update(@RequestBody Page page) {
        final Page newPage = repository.save(page);
        return new ResponseEntity<>(newPage, HttpStatus.OK);
    }

    public HttpEntity delete(@RequestBody Page page) {
        repository.delete(page);
        return new ResponseEntity(HttpStatus.OK);
    }
}
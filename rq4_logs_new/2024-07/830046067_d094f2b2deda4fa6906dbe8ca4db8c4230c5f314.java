package com.example.librarymanagement.controllers;

import com.example.librarymanagement.models.entities.Inventory;
import com.example.librarymanagement.models.entities.Library;
import com.example.librarymanagement.services.ILibraryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/library")
public class LibraryController {
    private final ILibraryService iLibraryService;

    public LibraryController(ILibraryService iLibraryService) {
        this.iLibraryService = iLibraryService;
    }

    @GetMapping
    public ResponseEntity<List<Library>> getAllLibraries() {
        return new ResponseEntity<>(iLibraryService.findAll(), HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Library> getById(@PathVariable Long id) {
        return new ResponseEntity<>(iLibraryService.getById(id), HttpStatus.OK);
    }

    @PostMapping("/create")
    public ResponseEntity<Library> createLibrary(@RequestBody Library library) {
        return new ResponseEntity<>(iLibraryService.createLibrary(library), HttpStatus.CREATED);
    }

    @PutMapping("/{id}/update")
    public ResponseEntity<Library> updateLibrary(@PathVariable Long id, @RequestBody Library library) {
        return new ResponseEntity<>(iLibraryService.updateLibrary(id, library), HttpStatus.OK);
    }

    @PostMapping("/{libraryId}/addbook/{bookId}")
    public ResponseEntity<Library> addBookToLibrary(@PathVariable Long libraryId, @PathVariable Long bookId) {
        return new ResponseEntity<>(iLibraryService.addBookToLibrary(libraryId, bookId), HttpStatus.OK);
    }

    @PutMapping("/{libraryId}/updatebook/{bookId}")
    public ResponseEntity<Library> updateBookInLibrary(@PathVariable Long libraryId,
                                                       @PathVariable Long bookId,
                                                       @RequestBody Inventory inventory) {
        return new ResponseEntity<>(iLibraryService.updateBookInLibrary(libraryId, bookId, inventory), HttpStatus.OK);
    }

    @DeleteMapping("/{libraryId}/deletebook/{bookId}")
    public ResponseEntity<Void> deleteBookFromLibrary(@PathVariable Long libraryId, @PathVariable Long bookId) {
        iLibraryService.removeBookFromLibrary(libraryId, bookId);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @DeleteMapping("/{id}/delete")
    public ResponseEntity<Library> deleteLibrary(@PathVariable Long id) {
        iLibraryService.deleteLibrary(id);
        return new ResponseEntity<>(HttpStatus.OK);
    }
}
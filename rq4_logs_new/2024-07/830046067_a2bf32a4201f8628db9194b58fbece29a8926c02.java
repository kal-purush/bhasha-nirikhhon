package com.example.librarymanagement.controller;

import com.example.librarymanagement.dto.BookPresenceDto;
import com.example.librarymanagement.dto.mapper.BookMapper;
import com.example.librarymanagement.dto.mapper.BookPresenceMapper;
import com.example.librarymanagement.model.enums.Availability;
import com.example.librarymanagement.service.BookPresenceService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/library")
@RequiredArgsConstructor
public class BookPresenceController {
    private final BookPresenceService bookPresenceService;
    private final BookPresenceMapper bookPresenceMapper;

    @PostMapping("/{libraryId}/book/{bookId}/presence")
    @ResponseStatus(HttpStatus.CREATED)
    public BookPresenceDto addBookToLibrary(@PathVariable Long libraryId, @PathVariable Long bookId) {
        return bookPresenceMapper.toBookPresenceDto(bookPresenceService.addBookToLibrary(libraryId, bookId));
    }

    @GetMapping("/{libraryId}/book")
    @ResponseStatus(HttpStatus.OK)
    public List<BookPresenceDto> getAllBooksByLibraryId(@PathVariable Long libraryId,
                                                        @RequestParam(required = false) Availability availability) {
        return bookPresenceMapper.toBookPresenceDto(availability == null
                        ? bookPresenceService.getByLibraryId(libraryId)
                        : bookPresenceService.getAllBookByLibraryIdAndAvailability(libraryId, availability)
        );
    }

    @GetMapping("/{libraryId}/book/{bookId}/presence")
    @ResponseStatus(HttpStatus.OK)
    public List<BookPresenceDto> getAllBooksByLibraryIdAndBookId(@PathVariable Long libraryId, @PathVariable Long bookId) {
        return bookPresenceMapper.toBookPresenceDto(bookPresenceService.getAllBookByLibraryIdAndBookId(libraryId, bookId));
    }

    @DeleteMapping("/{libraryId}/book/{bookId}/presence")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void removeBookFromLibrary(@PathVariable Long libraryId, @PathVariable(name = "bookId") Long presenceId) {
        bookPresenceService.deleteBookPresenceByIdAndLibraryId(libraryId, presenceId);
    }
}
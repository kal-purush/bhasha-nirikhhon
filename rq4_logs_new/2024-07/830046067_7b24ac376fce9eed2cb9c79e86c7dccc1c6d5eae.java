package com.example.librarymanagement.service;

import com.example.librarymanagement.model.entity.BookPresence;
import com.example.librarymanagement.model.entity.User;
import com.example.librarymanagement.model.enums.Availability;

import java.util.List;

public interface BookPresenceService {
    BookPresence createBookPresence(BookPresence bookPresence);
    BookPresence addUserToBook(User user, Long libraryId, Long bookId);
    BookPresence removeUserFromBook(User user, Long libraryId, Long bookId);
    List<BookPresence> getByBookId(Long bookId);
    List<BookPresence> getByLibraryId(Long libraryId);
    List<BookPresence> getAllBookByLibraryIdAndBookId(Long libraryId, Long bookId);
    List<BookPresence> getAllBookByLibraryIdAndAvailability(Long libraryId, Availability availability);
    void deleteBookPresenceById(Long id);
}
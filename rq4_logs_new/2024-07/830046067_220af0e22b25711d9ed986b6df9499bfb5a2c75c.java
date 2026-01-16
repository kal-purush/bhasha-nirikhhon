package com.example.librarymanagement.repository;

import com.example.librarymanagement.model.entity.BookPresence;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface BookPresenceRepository extends JpaRepository<BookPresence, Long> {
    List<BookPresence> findByLibraryId(Long libraryId);
    List<BookPresence> findAllByBookId(Long bookId);
    List<BookPresence> findAllByLibraryId(Long libraryId);
    List<BookPresence> findAllByUserId(Long userId);
    List<BookPresence> findAllByLibraryIdAndBookId(Long libraryId, Long bookId);
}
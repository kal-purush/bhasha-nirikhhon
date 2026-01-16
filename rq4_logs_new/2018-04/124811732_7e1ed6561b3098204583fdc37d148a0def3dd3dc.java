/*
 * Copyright (c) wheejuni tech 2018.
 *
 * Proudly developed by Hwi Jun Jeong,
 * Inspired by Bomee, the smartest puppy of the Galaxy.
 *
 * me@wheejuni.com
 * https://github.com/seulgiwendy
 */

package com.codesquad.library.domain.service.book;

import com.codesquad.library.domain.Book;
import com.codesquad.library.domain.Tag;
import com.codesquad.library.domain.TagSet;
import com.codesquad.library.domain.exceptions.model.NoBookExistsException;
import com.codesquad.library.domain.repositories.BookRepository;
import com.codesquad.library.domain.repositories.TagRepository;
import com.codesquad.library.dtos.model.tag.NewTagDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Transactional
public class BookTagService {

    @Autowired
    private BookRepository bookRepository;

    @Autowired
    private TagRepository tagRepository;

    public Book addTag(NewTagDocument document) {
        Book targetBook = bookRepository.findById(document.getId()).orElseThrow(() -> new NoBookExistsException("ID에 해당하는 도서가 없습니다."));
        Tag tag = tagRepository.findByName(document.getTitle()).orElse(new Tag(0L, document.getTitle(), null));

        TagSet set = new TagSet(0L, targetBook, tag);

        targetBook.addTagSet(set);
        return targetBook;
    }
}
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
import com.codesquad.library.dtos.model.tag.NewTagDocument;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.*;

@SpringBootTest
@RunWith(SpringRunner.class)
public class BookTagServiceTest {

    @Autowired
    private BookTagService service;

    private Book book;
    private NewTagDocument document;

    @Before
    public void setUp() {
        this.book = Book.builder().title("이말년씨리즈").description("description").build();
    }

    @Test
    public void TAG_PERSIST_TEST() {

    }

}
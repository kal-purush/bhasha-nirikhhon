package com.ssu.commerce.book.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ssu.commerce.book.dto.BookDetailDto;
import com.ssu.commerce.book.dto.BookDto;
import com.ssu.commerce.book.dto.request.DeleteBookRequestDto;
import com.ssu.commerce.book.dto.response.ChangeBookResponseDto;
import com.ssu.commerce.book.service.BookService;
import com.ssu.commerce.book.supplier.BookControllerTestDataSupplier;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;


@RunWith(SpringRunner.class)
@WebMvcTest(BookController.class)
@WithMockUser(username = "tester", roles = "USER")
class BookControllerTest implements BookControllerTestDataSupplier {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private BookService bookService;

    @Test
    void getBookList() {

        List<BookDto> bookDtoList = Arrays.asList(
                BookControllerTestDataSupplier.getBookDto(),
                BookControllerTestDataSupplier.getAnotherBookDto()
        );
        Page<BookDto> page = new PageImpl<>(bookDtoList);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

        when(bookService.getBookList(any())).thenReturn(page);

        assertDoesNotThrow(() -> {
            mockMvc.perform(get("/v1/book"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.content", hasSize(2)))
                    .andExpect(jsonPath("$.content[0].id", equalTo(String.valueOf(TEST_VAL_BOOK_ID))))
                    .andExpect(jsonPath("$.content[0].title", equalTo(TEST_VAL_BOOK_TITLE)))
                    .andExpect(jsonPath("$.content[0].content", equalTo(TEST_VAL_BOOK_CONTENT)))
                    .andExpect(jsonPath("$.content[0].writer", equalTo(TEST_VAL_BOOK_WRITER)))
                    .andExpect(jsonPath("$.content[0].price", equalTo(TEST_VAL_BOOK_PRICE.intValue())))
                    .andExpect(jsonPath("$.content[0].ownerId", equalTo(String.valueOf(TEST_VAL_OWNER_ID))))
                    .andExpect(jsonPath("$.content[0].publishDate", equalTo(TEST_VAL_BOOK_PUBLISH_DATE.format(formatter))))
                    .andExpect(jsonPath("$.content[0].isbn", equalTo(TEST_VAL_BOOK_ISBN)))
                    .andExpect(jsonPath("$.content[0].maxBorrowDay", equalTo(TEST_VAL_BOOK_MAX_BORROW_DAY.intValue())))
                    .andExpect(jsonPath("$.content[0].categoryId", equalTo(String.valueOf(TEST_VAL_BOOK_CATEGORY_ID))))
                    .andExpect(jsonPath("$.content[1].id", equalTo(String.valueOf(TEST_VAL_ANOTHER_BOOK_DTO_ID))))
                    .andExpect(jsonPath("$.content[1].title", equalTo(TEST_VAL_BOOK_TITLE)))
                    .andExpect(jsonPath("$.content[1].content", equalTo(TEST_VAL_BOOK_CONTENT)))
                    .andExpect(jsonPath("$.content[1].writer", equalTo(TEST_VAL_BOOK_WRITER)))
                    .andExpect(jsonPath("$.content[1].price", equalTo(TEST_VAL_BOOK_PRICE.intValue())))
                    .andExpect(jsonPath("$.content[1].ownerId", equalTo(String.valueOf(TEST_VAL_ANOTHER_OWNER_ID))))
                    .andExpect(jsonPath("$.content[1].publishDate", equalTo(TEST_VAL_BOOK_PUBLISH_DATE.format(formatter))))
                    .andExpect(jsonPath("$.content[1].isbn", equalTo(TEST_VAL_BOOK_ISBN)))
                    .andExpect(jsonPath("$.content[1].maxBorrowDay", equalTo(TEST_VAL_BOOK_MAX_BORROW_DAY.intValue())))
                    .andExpect(jsonPath("$.content[1].categoryId", equalTo(String.valueOf(TEST_VAL_BOOK_CATEGORY_ID))));
            verify(bookService, times(1)).getBookList(any());
        });

    }

    @Test
    void getBookDetail() {

        BookDetailDto bookDetailDto = BookControllerTestDataSupplier.getBookDetailDto();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

        when(bookService.getBookDetail(any())).thenReturn(bookDetailDto);

        assertDoesNotThrow(() -> {
            mockMvc.perform(get("/v1/book/" + TEST_VAL_BOOK_ID)
                            .accept(MediaType.APPLICATION_JSON))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.id", equalTo(String.valueOf(TEST_VAL_BOOK_ID))))
                        .andExpect(jsonPath("$.title", equalTo(TEST_VAL_BOOK_TITLE)))
                        .andExpect(jsonPath("$.content", equalTo(TEST_VAL_BOOK_CONTENT)))
                        .andExpect(jsonPath("$.writer", equalTo(TEST_VAL_BOOK_WRITER)))
                        .andExpect(jsonPath("$.price", equalTo(TEST_VAL_BOOK_PRICE.intValue())))
                        .andExpect(jsonPath("$.ownerId", equalTo(String.valueOf(TEST_VAL_OWNER_ID))))
                        .andExpect(jsonPath("$.publishDate", equalTo(TEST_VAL_BOOK_PUBLISH_DATE.format(formatter))))
                        .andExpect(jsonPath("$.isbn", equalTo(TEST_VAL_BOOK_ISBN)))
                        .andExpect(jsonPath("$.maxBorrowDay", equalTo(TEST_VAL_BOOK_MAX_BORROW_DAY.intValue())))
                        .andExpect(jsonPath("$.categoryId", equalTo(String.valueOf(TEST_VAL_BOOK_CATEGORY_ID))));
        });
    }

    @Test
    void registerBook() {
        UUID uuid = UUID.randomUUID();

        when(bookService.registerBook(any())).thenReturn(uuid);

        assertDoesNotThrow(() -> {
            mockMvc.perform(post("/v1/book")
                            .content(new ObjectMapper().registerModule(
                                    new JavaTimeModule()).writeValueAsString(
                                            BookControllerTestDataSupplier.getRegisterBookRequestDto()
                                    ))
                            .contentType(MediaType.APPLICATION_JSON)
                            .with(csrf())
                        .accept(MediaType.APPLICATION_JSON))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.id", equalTo(String.valueOf(uuid))));
        });
    }

    @Test
    void changeBook() {

        ChangeBookResponseDto changeBookResponseDto = BookControllerTestDataSupplier.getChangeBookResponseDto();

        when(bookService.changeBook(any())).thenReturn(changeBookResponseDto.getId());

        assertDoesNotThrow(() -> {
            mockMvc.perform(put("/v1/book")
                            .content(new ObjectMapper().registerModule(
                                    new JavaTimeModule()).writeValueAsString(
                                    BookControllerTestDataSupplier.getRegisterBookRequestDto()
                            ))
                            .contentType(MediaType.APPLICATION_JSON)
                            .with(csrf())
                            .accept(MediaType.APPLICATION_JSON))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.id", equalTo(String.valueOf(changeBookResponseDto.getId()))));
        });
    }

    @Test
    void deleteBook() {

        DeleteBookRequestDto deleteBookRequestDto = BookControllerTestDataSupplier.getDeleteBookRequestDto();

        when(bookService.deleteBook(any())).thenReturn(deleteBookRequestDto.getId());

        assertDoesNotThrow(() -> {
            mockMvc.perform(delete("/v1/book")
                            .content(new ObjectMapper().registerModule(
                                    new JavaTimeModule()).writeValueAsString(deleteBookRequestDto))
                            .contentType(MediaType.APPLICATION_JSON)
                            .with(csrf())
                            .accept(MediaType.APPLICATION_JSON))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.id", equalTo(String.valueOf(deleteBookRequestDto.getId()))));
        });
    }
}
package com.practice.practiceProject.controller;

import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.practiceProject.Security.JwtAuthFilter;
import com.practice.practiceProject.TestContainerBase;
import com.practice.practiceProject.constant.BookConstant;
import com.practice.practiceProject.constant.MessageConstant;
import com.practice.practiceProject.dto.BookDto;
import com.practice.practiceProject.dto.BookUpdateDto;
import com.practice.practiceProject.dto.UserDto;
import com.practice.practiceProject.dto.UserInputDto;
import com.practice.practiceProject.entities.Book;
import com.practice.practiceProject.entities.User;
import com.practice.practiceProject.enums.ErrorEnum;
import com.practice.practiceProject.exception.UserNotFoundException;
import com.practice.practiceProject.repository.UserRepository;
import com.practice.practiceProject.response.BookResponse;
import com.practice.practiceProject.response.ErrorResponse;
import com.practice.practiceProject.response.PracticeProjectResponse;
import com.practice.practiceProject.service.serviceImpl.ShopOwnerServiceImpl;
import com.practice.practiceProject.service.serviceImpl.UserServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@WithMockUser
@SpringBootTest
public class ShopOwnerControllerTest extends TestContainerBase {
    public static final String POST_ADD_BOOK = "/shopowner/addbook";
    public static final String PUT_UPDATE_BOOK = "/shopowner/updateBook";
    public static final String DELETE_BOOK = "/shopowner/deleteBook";

    @Autowired
    protected WebApplicationContext context;

    @Autowired
    private ObjectMapper objectMapper;

    private MockMvc mockMvc;

    @MockBean
    private JwtAuthFilter jwtAuthFilter;

    @MockBean
    private ShopOwnerServiceImpl shopOwnerServiceImpl;


    @BeforeEach
    public void setUp() {
        try {
            if (null == mockMvc) {
                mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void invalidAddBook() throws Exception {
        BookDto bookDto = new BookDto();
        bookDto.setTitle("");
        bookDto.setAuthorName("");
        bookDto.setPrice(null);
        bookDto.setRentalPrice(null);
        bookDto.setStock(null);

        mockMvc.perform(MockMvcRequestBuilders.post(POST_ADD_BOOK)
                        .content(objectMapper.writeValueAsString(bookDto))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value(BookConstant.SHORT_BOOK_TITLE))
                .andExpect(jsonPath("$.authorName").value(BookConstant.SHORT_AUTHOR_NAME))
                .andExpect(jsonPath("$.price").value(BookConstant.NULL_BOOK_PRICE))
                .andExpect(jsonPath("$.rentalPrice").value(BookConstant.NULL_RENTAL_PRICE))
                .andExpect(jsonPath("$.stock").value(BookConstant.NULL_BOOK_STOCK));
    }

    @Test
    void validAddBook() throws Exception {
        BookDto bookDto = new BookDto();
        bookDto.setTitle("testBook");
        bookDto.setAuthorName("testAuthor");
        bookDto.setPrice(100.00);
        bookDto.setRentalPrice(80.00);
        bookDto.setStock(10);

        Book book = new Book();
        book.setId(1);
        book.setTitle("testBookNew");
        book.setAuthorName("testAuthor");
        book.setIsbn("123456");
        book.setPrice(100.00);
        book.setRentalPrice(80.00);
        book.setStock(10);

        final BookResponse bookResponse = new BookResponse(new Book(),BookConstant.BOOK_ADDED_SUCCESS, true);
        System.out.println(bookResponse);


        when(shopOwnerServiceImpl.addBook(new BookDto())).thenReturn(bookResponse);



        mockMvc.perform(MockMvcRequestBuilders.post(POST_ADD_BOOK)
                        .with(csrf())
                        .content(objectMapper.writeValueAsString(new BookDto()))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.message").value(BookConstant.BOOK_ADDED_SUCCESS));
//                .andExpect(jsonPath("$.success").value(true))
//                .andExpect(jsonPath("$.book.id").value(1));
    }

    @Test
    void invalid_updateBook() throws Exception {
        BookUpdateDto bookUpdateDto = new BookUpdateDto();
        bookUpdateDto.setTitle("");
        bookUpdateDto.setAuthorName("");
        bookUpdateDto.setPrice(null);
        bookUpdateDto.setRentalPrice(null);
        bookUpdateDto.setStock(null);

        Integer id = 1;

        mockMvc.perform(MockMvcRequestBuilders.put(PUT_UPDATE_BOOK)
                        .param("id", String.valueOf(id))
                        .content(objectMapper.writeValueAsString(bookUpdateDto))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.rentalPrice").value(BookConstant.NULL_RENTAL_PRICE))
                .andExpect(jsonPath("$.price").value(BookConstant.NULL_BOOK_PRICE))
                .andExpect(jsonPath("$.authorName").value(BookConstant.SHORT_AUTHOR_NAME))
                .andExpect(jsonPath("$.title").value(BookConstant.SHORT_BOOK_TITLE))
                .andExpect(jsonPath("$.stock").value(BookConstant.NULL_BOOK_STOCK));
    }

    @Test
    void deleteBook_positiveScenario() throws Exception {
        BookResponse bookResponse = new BookResponse(BookConstant.BOOK_DELETE_SUCCESS, true);
        when(shopOwnerServiceImpl.deleteBook(1)).thenReturn(bookResponse);
        System.out.println(bookResponse);
        mockMvc.perform(MockMvcRequestBuilders.delete(DELETE_BOOK)
                        .param("id", String.valueOf(1)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.message").value(BookConstant.BOOK_DELETE_SUCCESS))
                .andExpect(jsonPath("$.success").value(true));

    }
}
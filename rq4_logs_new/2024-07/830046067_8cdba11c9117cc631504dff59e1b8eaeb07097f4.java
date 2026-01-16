package com.example.librarymanagement.controller;

import com.example.librarymanagement.dto.AuthorDto;
import com.example.librarymanagement.exception.EntityNotFoundException;
import com.example.librarymanagement.model.entity.Author;
import com.example.librarymanagement.service.AuthorService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AuthorController.class)
public class AuthorControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private AuthorService authorService;

    @Autowired
    private ObjectMapper objectMapper;

    private static Author author1;
    private static Author author2;
    private static AuthorDto authorDto1;

    @BeforeEach
    public void init() {
        author1 = new Author();
        author1.setId(1L);
        author1.setFirstName("Author");
        author1.setLastName("Author");
        author1.setPseudonym("Author");

        author2 = new Author();
        author2.setId(2L);
        author2.setFirstName("Author");
        author2.setLastName("Author");
        author2.setPseudonym("Author");

        authorDto1 = new AuthorDto(author1);
    }

    @Test
    void createAuthor() throws Exception {
        String expected = objectMapper.writeValueAsString(authorDto1);

        given(authorService.createAuthor(any(Author.class))).willReturn(author1);

        String result = mockMvc.perform(post("/api/v1/author")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(authorDto1)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        assertEquals(expected, result);
    }

    @Test
    void createAuthorWithInvalidData() throws Exception {
        authorDto1.setLastName("author");

        given(authorService.createAuthor(any(Author.class))).willThrow(new IllegalArgumentException());

        mockMvc.perform(post("/api/v1/author")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(authorDto1)))
                .andExpect(status().isBadRequest())
                .andReturn().getResponse().getContentAsString();
    }

    @Test
    void updateAuthor() throws Exception {
        String expected = objectMapper.writeValueAsString(authorDto1);

        given(authorService.updateAuthor(anyLong(), any(Author.class))).willReturn(author1);

        String result = mockMvc.perform(put("/api/v1/author/{id}", 1L)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(authorDto1)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertEquals(expected, result);
    }

    @Test
    void updateAuthorWithInvalidData() throws Exception {
        authorDto1.setLastName("author");

        given(authorService.updateAuthor(anyLong(), any(Author.class))).willThrow(new IllegalArgumentException());

        mockMvc.perform(put("/api/v1/author/{id}", 1L)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(authorDto1)))
                .andExpect(status().isBadRequest())
                .andReturn().getResponse().getContentAsString();
    }

    @Test
    void getAllAuthors() throws Exception {
        String expected = objectMapper.writeValueAsString(List.of(authorDto1, new AuthorDto(author2)));

        given(authorService.getAllAuthors()).willReturn(List.of(author1, author2));

        String result = mockMvc.perform(get("/api/v1/author/all"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertEquals(expected, result);
    }

    @Test
    void getAuthorById() throws Exception {
        String expected = objectMapper.writeValueAsString(authorDto1);

        given(authorService.getAuthorById(1L)).willReturn(author1);

        String result = mockMvc.perform(get("/api/v1/author/{id}", 1L))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertEquals(expected, result);
    }

    @Test
    void getAuthorByIdWithInvalidId() throws Exception {
        given(authorService.getAuthorById(1L)).willThrow(new EntityNotFoundException("Author"));

        mockMvc.perform(get("/api/v1/author/{id}", 1L))
                .andExpect(status().isNotFound())
                .andReturn().getResponse().getContentAsString();
    }
}
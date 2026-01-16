package com.example.blogit.blog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(BlogController.class)
public class BlogControllerUnitTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private BlogService blogService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    public void ShouldReturnBlogOnCreation() throws Exception {
        Blog blog = new Blog(1L, "A new blog", "this is a new blog", "bannerImgTest", LocalDateTime.now());
        when(blogService.createBlog(any(Blog.class))).thenReturn(blog);


        var result = this.mockMvc
                .perform(post("/api/blog/create")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(blog))
                );

        var expectedBlog = objectMapper.writeValueAsString(blog);
        result.andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().json(expectedBlog))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }

    @Test
    public void ShouldReturn400OnCreation() throws Exception {
        Blog blog = new Blog(1L, "", "this is a new blog", "bannerImgTest", LocalDateTime.now());
        when(blogService.createBlog(any(Blog.class))).thenReturn(blog);


        var result = this.mockMvc
                .perform(post("/api/blog/create")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(blog))
                );

        result.andDo(print())
                .andExpect(status().isBadRequest());
    }

    @Test
    public void ShouldGetAllBlogsPaginated() throws Exception {
        Blog blog = new Blog(1L, "Blog title", "this is a new blog", "bannerImgTest", LocalDateTime.now());
        BlogListDto blogListDto = new BlogListDto(Collections.singletonList(blog), true, 1, 0);

        when(blogService.getBlogs(0)).thenReturn(blogListDto);

        var result = this.mockMvc
                .perform(get("/api/blog/get-blogs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .param("page", "0")
                );

        var expectedBlogList = objectMapper.writeValueAsString(blogListDto);
        result.andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().json(expectedBlogList))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }
}
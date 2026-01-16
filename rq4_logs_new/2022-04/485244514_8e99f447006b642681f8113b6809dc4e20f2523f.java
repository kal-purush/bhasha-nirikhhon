package com.example.blogit.blog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@WebMvcTest(BlogService.class)
public class BlogServiceUnitTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private BlogService blogService;

    @MockBean
    private BlogRepository blogRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    public void shouldCreateBlog() throws Exception {
        Blog blog = new Blog(1L, "This is a blog", "this is a test blog content", "bannerImg", LocalDateTime.now());

        when(blogRepository.save(any(Blog.class))).thenReturn(blog);

        Blog createdBlog = blogService.createBlog(blog);
        assertThat(createdBlog.getUuid()).isSameAs(blog.getUuid());
        assertThat(createdBlog.getTitle()).isSameAs(blog.getTitle());
    }

    @Test
    public void shouldCreateBlogAndFail() throws Exception {
        Blog blog = new Blog(1L, "This is a blog", "this is a test blog content", "bannerImg", LocalDateTime.now());

        when(blogRepository.save(any(Blog.class))).thenReturn(blog);
        when(blogRepository.findByTitle(any(String.class))).thenReturn(Optional.of(blog));

        Exception exception = assertThrows(Exception.class, () -> {
            blogService.createBlog(blog);
        });


        String expectedMessage = "Blog already exists";
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    public void blogWithTitleExistsShouldReturnBoolean() {
        when(blogRepository.findByTitle(any(String.class))).thenReturn(Optional.empty());

        boolean result = blogService.blogWithTitleExists("a new blog");
        assertFalse(result);
    }

    @Test
    public void mapBlogsToBlogListDtoShouldReturnBlogListDto() {
        Blog blog = new Blog(1L, "This is a blog", "this is a test blog content", "bannerImg", LocalDateTime.now());
        List<Blog> blogList = List.of(blog);

        Page<Blog> blogPage = new PageImpl<>(blogList);
        BlogListDto blogs = blogService.mapBlogsToBlogListDto(blogPage);
        assertThat(blogs.getTotalPages()).isSameAs(1);
    }

    @Test
    public void gettingAllBlogsPaginatedShouldReturnBlogs() {
        Blog blog = new Blog(
                1L,
                "This is a blog",
                "this is a test blog content",
                "bannerImg",
                LocalDateTime.now()
        );
        List<Blog> blogList = List.of(blog);
        Page<Blog> blogPage = new PageImpl<>(blogList);

        when(blogRepository.findAll(any(PageRequest.class))).thenReturn(blogPage);

        BlogListDto blogs = blogService.getBlogs(0);
        assertThat(blogs.getBlogs().get(0).getTitle()).isSameAs(blog.getTitle());
        assertThat(blogs.getCurrentPage()).isSameAs(0);
        assertThat(blogs.getTotalPages()).isSameAs(1);
        assertThat(blogs.getIsLastPage()).isSameAs(true);
    }

    @Test
    public void getBlogShouldReturnSingleBlog() {
        UUID uuid = UUID.randomUUID();
        Blog blog = new Blog(
                uuid,
                1L,
                "This is a blog",
                "this is a test blog content",
                "bannerImg",
                LocalDateTime.now()
        );

        when(blogRepository.findBlogByUuid(any(UUID.class))).thenReturn(blog);

        Blog foundBlog = blogService.getBlog(uuid.toString());
        assertThat(foundBlog.getUuid()).isSameAs(blog.getUuid());
        assertThat(foundBlog.getTitle()).isSameAs(blog.getTitle());
    }

}
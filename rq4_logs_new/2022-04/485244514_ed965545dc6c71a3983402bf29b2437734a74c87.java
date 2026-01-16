package com.example.blogit.comment;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(CommentController.class)
public class CommentControllerUnitTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private CommentService commentService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    public void shouldCreateComment() throws Exception {
        Comment comment = new Comment(1L, "this is a message", UUID.randomUUID());

        when(commentService.createComment(any(Comment.class))).thenReturn(comment);

        var result = this.mockMvc.perform(post("/api/blog/comments/create")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(comment))
        );

        result
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().json(objectMapper.writeValueAsString(comment)))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }

    @Test
    public void shouldFailToCreateCommentWithBlankMessage() throws Exception {
        Comment comment = new Comment(1L, "", UUID.randomUUID());

        when(commentService.createComment(any(Comment.class))).thenReturn(comment);

        var result = this.mockMvc.perform(post("/api/blog/comments/create")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(comment))
        );

        result
                .andDo(print())
                .andExpect(status().isBadRequest());
    }

    @Test
    public void shouldUpvoteComment() throws Exception {
        String blogUUID = UUID.randomUUID().toString();

        Comment comment = new Comment(1L, "This is a message", UUID.fromString(blogUUID));
        comment.setUpvotes(comment.getUpvotes() + 1);

        when(commentService.upvoteComment(any(UUID.class))).thenReturn(comment);

        var result = this.mockMvc.perform(put("/api/blog/comments/upvote/" + comment.getUuid())
                .contentType(MediaType.APPLICATION_JSON)
        );

        result
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().json(objectMapper.writeValueAsString(comment)))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }

    @Test
    public void shouldGetBlogComments() throws Exception {
        String blogUUID = UUID.randomUUID().toString();
        List<Comment> blogComments = List.of(
                new Comment(1L, "This is a message", UUID.fromString(blogUUID)),
                new Comment(1L, "This is a second message", UUID.fromString(blogUUID))
        );


        when(commentService.getComments(any(UUID.class))).thenReturn(blogComments);

        var result = this.mockMvc.perform(get("/api/blog/comments/" + blogUUID)
                .contentType(MediaType.APPLICATION_JSON)
        );

        result
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().json(objectMapper.writeValueAsString(blogComments)))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }
}
package com.company.getyourgoal.controller;

import com.company.getyourgoal.model.Comment;
import com.company.getyourgoal.repository.CommentRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Optional;

import static junit.framework.TestCase.*;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class CommentControllerTest {

    @Autowired
    CommentRepository commentRepository;
    @Before
    public void setUp() throws Exception {
        
        commentRepository.deleteAll();
    }

    @Test
    public void addGetDeleteComment() {
        Comment comment = new Comment();
        comment.setComment("some comment");
        comment.setUserId(2);
        comment.setGoalId(2);
        commentRepository.save(comment);

        Optional <Comment> comment1 = commentRepository.findById(comment.getId());
        assertEquals(comment1.get(), comment);
        commentRepository.deleteById(comment.getId());
        comment1 = commentRepository.findById(comment.getId());
        assertFalse(comment1.isPresent());
    }

    @Test
    public void getAllComment() {
        Comment comment = new Comment();
        comment.setComment("some comment");
        comment.setUserId(2);
        comment.setGoalId(2);
        commentRepository.save(comment);

        comment = new Comment();
        comment.setComment("Another comment");
        comment.setUserId(3);
        comment.setGoalId(5);
        commentRepository.save(comment);

        List<Comment> commentList = commentRepository.findAll();
        assertEquals(commentList.size(), 2);

    }

    @Test
    public void UpdateComment() {
        Comment comment = new Comment();
        comment.setComment("some comment");
        comment.setUserId(2);
        comment.setGoalId(2);
        commentRepository.save(comment);

        comment.setComment("replaced comment");
        comment.setUserId(2);
        comment.setGoalId(2);
        commentRepository.save(comment);

        Optional<Comment> comment1 = commentRepository.findById(comment.getId());

        assertEquals(comment1.get(), comment);
    }

    @Test
    public void getOneByID() {

        Comment comment = new Comment();
        comment.setComment("some usr comment");
        comment.setUserId(6);
        comment.setGoalId(3);
        commentRepository.save(comment);

        Optional <Comment> comment1 = commentRepository.findById(comment.getId());
        assertEquals(comment1.get(), comment);
        comment1 = commentRepository.findById(comment.getId());
        assertTrue(comment1.isPresent());

    }
}
package com.revature.Service;

import com.revature.models.Like;
import com.revature.models.Post;
import com.revature.models.User;
import com.revature.repositories.LikeRepository;
import com.revature.services.LikeService;
import com.revature.services.PostService;
import com.revature.services.UserService;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

public class LikeServiceTest {
    @Mock
    private PostService postService;
    @Mock
    private LikeRepository likesRepo;
    @Mock
    private UserService userService;
    @InjectMocks
    private LikeService service;



}
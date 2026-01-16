package com.phongvdoan.codefellowship.controllers;

import com.phongvdoan.codefellowship.models.ApplicationUser;
import com.phongvdoan.codefellowship.models.ApplicationUserRepository;
import com.phongvdoan.codefellowship.models.Post;
import com.phongvdoan.codefellowship.models.PostRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.view.RedirectView;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

@Controller
public class PostController {

    @Autowired
    ApplicationUserRepository applicationUserRepository;

    @Autowired
    PostRepository postRepository;

    //https://www.javatpoint.com/java-date-to-string
    @PostMapping("/post")
    public RedirectView createNewPost(long id, String message, String body){
        System.out.println(id);
        Date date = Calendar.getInstance().getTime();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
        String createdAt = dateFormat.format(date);
        ApplicationUser applicationUser = applicationUserRepository.findById(id).get();
        Post newPost = new Post(applicationUser, message, body, createdAt);
        postRepository.save(newPost);
        return new RedirectView("/myprofile");
    }
}
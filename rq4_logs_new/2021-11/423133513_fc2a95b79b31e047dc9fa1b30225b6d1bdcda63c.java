package com.example.codefellowship.Controllers;

import com.example.codefellowship.Model.ApplicationUser;
import com.example.codefellowship.Model.UserPost;
import com.example.codefellowship.Repositories.AppUserRepository;
import com.example.codefellowship.Repositories.PostUserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.view.RedirectView;

import java.security.Principal;
import java.util.List;

@Controller
public class PostController {

    @Autowired
    AppUserRepository appUserRepository;

    @Autowired
    PostUserRepository postUserRepository;



    @Autowired
    PasswordEncoder encoder;



    @GetMapping("/post")
    public String getPost(Principal principal, Model model) {
        ApplicationUser appUser = appUserRepository.findByUsername(principal.getName());
        List<UserPost> userPosts=postUserRepository.findAllByApplicationUser(appUser);
        model.addAttribute("posts", userPosts);

        return "post";
    }

    @PostMapping("/post")
    public RedirectView createPost(String body, Principal principal) {
        ApplicationUser applicationUser = appUserRepository.findByUsername(principal.getName());
        UserPost post = new UserPost( applicationUser,body);
        postUserRepository.save(post);
        return new RedirectView("/post");
    }


}
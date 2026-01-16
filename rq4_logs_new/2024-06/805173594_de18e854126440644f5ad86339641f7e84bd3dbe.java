package com.example.BookingTravelBackend.controllers;

import com.example.BookingTravelBackend.entity.User;
import com.example.BookingTravelBackend.payload.Request.ReviewRequest;
import com.example.BookingTravelBackend.payload.respone.HttpRespone;
import com.example.BookingTravelBackend.service.ReviewService;
import com.example.BookingTravelBackend.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping ("/review")
public class ReviewController {
    private final ReviewService reviewService;
    private final UserService userService;
    @GetMapping ("/getAll")
    public ResponseEntity<HttpRespone> getAll (){
        return ResponseEntity.ok(new HttpRespone(HttpStatus.OK.value(),"success",reviewService.selectAll()));
    }

    @PostMapping ("/comment")
    public ResponseEntity<HttpRespone> comment (@RequestBody ReviewRequest request){
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        User principal = (User) authentication.getPrincipal();
        User userLogin = userService.findById(principal.getId());
        return ResponseEntity.ok(new HttpRespone(HttpStatus.OK.value(),"success",reviewService.Comment(request,userLogin)));
    }
}
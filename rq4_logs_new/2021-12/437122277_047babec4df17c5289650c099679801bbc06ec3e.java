package com.example.onlineshopping.controller;

import com.example.onlineshopping.dto.UserDto;
import com.example.onlineshopping.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/users") @RequiredArgsConstructor
public class UserController {
    private final UserService userService;
    @GetMapping("{id}")
    public ResponseEntity<EntityModel<UserDto>> findUserById(@PathVariable("id") long id){
        UserDto userDto = userService.findById(id);
        EntityModel<UserDto> userDtoEntityModel = EntityModel.of(userDto);
        WebMvcLinkBuilder linkBuilder = WebMvcLinkBuilder.linkTo(WebMvcLinkBuilder.methodOn(this.getClass()).findUserById(userDto.getId()));
        userDtoEntityModel.add(linkBuilder.withRel("self"));
        return ResponseEntity.ok().body(userDtoEntityModel);
    }

}
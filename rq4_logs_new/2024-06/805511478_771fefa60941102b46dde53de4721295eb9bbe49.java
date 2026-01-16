package com.gpb.controller;

import com.gpb.entity.AccountRequestDto;
import com.gpb.entity.ResponseDto;
import com.gpb.exception.UserNotFoundException;
import com.gpb.service.AccountService;
import com.gpb.service.UserService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("v2/")
public class CreateAccountController {
    private final AccountService accountService;
    private final UserService userService;

    public CreateAccountController(AccountService accountService, UserService userService) {
        this.accountService = accountService;
        this.userService = userService;
    }

    @PostMapping("/users/{id}/accounts")
    public ResponseEntity<?> createAccount(@PathVariable("id") long userId, @RequestBody @Valid AccountRequestDto accountRequestDto) {
        if (userService.findById(userId).isEmpty()) {
            throw new UserNotFoundException("User doesn't exist, please register first");
        }
        ResponseDto response = accountService.createAccount(userId, accountRequestDto.getAccountName());
        if (response.getMessage().equals("Account created successfully")) {
            return new ResponseEntity<>(response, HttpStatus.NO_CONTENT);
        }
        return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
package azmain.github.io.controller;

import azmain.github.io.service.user.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping(path = "user", produces = MediaType.APPLICATION_JSON_VALUE)
public class UserController {

    @Autowired
    private UserService userService;

    @GetMapping(path = "{username}")
    public ResponseEntity<?> getUserDetailsByUsername(
            @PathVariable(name = "username") final String userName
    ){
        return ResponseEntity.ok(userService.getAppUserByUsername(userName));
    }
}
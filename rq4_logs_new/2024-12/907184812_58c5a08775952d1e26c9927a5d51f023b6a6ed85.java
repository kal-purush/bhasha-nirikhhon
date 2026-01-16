package tabom.myhands.domain.user.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tabom.myhands.common.response.DtoResponse;
import tabom.myhands.common.response.MessageResponse;
import tabom.myhands.domain.user.dto.UserRequest;
import tabom.myhands.domain.user.entity.User;
import tabom.myhands.domain.user.service.UserService;

@RestController
@RequestMapping("/user")
public class UserController {
    private final UserService userService;

    public UserController(UserService userService) {
        this.userService = userService;
    }

    @PostMapping("/join")
    public ResponseEntity<MessageResponse> joinUser(@RequestBody UserRequest.Join request) {
        userService.join(request);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(DtoResponse.of(HttpStatus.CREATED, "회원 가입 성공", null));
    }

    @GetMapping("/{id}")
    public ResponseEntity<MessageResponse> getUserById(@PathVariable Long id) {
        User user = userService.getUserById(id);
        return ResponseEntity
                .ok(DtoResponse.of(HttpStatus.OK, "회원 조회 성공", user));
    }
}
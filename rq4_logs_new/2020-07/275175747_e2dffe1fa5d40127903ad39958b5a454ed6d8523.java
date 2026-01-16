package jm.stockx.controller.rest;

import jm.stockx.UserService;
import jm.stockx.dto.UserDTO;
import jm.stockx.entity.User;
import jm.stockx.util.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@RestController
@RequestMapping("/registration")
@Slf4j
public class RegistrationRestController {
    private final UserService userService;

    @Autowired
    public RegistrationRestController(UserService userService) {
        this.userService = userService;
    }

    @PostMapping
    public Response<?> registerUser(@RequestBody @Valid UserDTO userDto,
                                      BindingResult result){
        if (result.hasErrors()) {
            log.info("Ошибка регистрации пользователя. Не корректные регистрационные данные.");
            return Response.error(HttpStatus.BAD_REQUEST, "Invalid registration data");
        }
        if (userService.getUserByEmail(userDto.getEmail()) != null) {
            log.info("Ошибка регистрации пользователя. Почтовый адрес {} уже используется.", userDto.getEmail());
            return Response.error(HttpStatus.BAD_REQUEST, "There is already an account associated with this email address. Please login using your existing account");
        }
        User user = new User(userDto);
        userService.createUser(user);
        log.info("Пользователь {} успешно зарегистрирован", user);
        return Response.ok(user);
    }

}
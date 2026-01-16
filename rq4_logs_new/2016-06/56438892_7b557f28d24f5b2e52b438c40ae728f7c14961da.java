package my.test.controller;

import my.test.dto.UserDto;
import my.test.dto.converter.UserConverter;
import my.test.model.entity.User;
import my.test.service.user.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Dragan Susak on 06-Jun-16.
 */
@RestController
@RequestMapping("/users")
public class UserController {

    @Autowired
    private UserService userService;

    @Autowired
    private UserConverter userConverter;

    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<UserDto>> getAllUsers()  throws Exception {
        return new ResponseEntity<>(userService.getAll().stream().map(user ->  userConverter.convertToDto(user)).collect(Collectors.toList()), HttpStatus.OK);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<UserDto> getUser(@PathVariable final int id) {
        return new ResponseEntity<>(userConverter.convertToDto(userService.getById(id)), HttpStatus.OK);
    }

    @RequestMapping(value = "/new", method = RequestMethod.POST)
    public ResponseEntity<Void> storeUser(@RequestBody @Valid UserDto user, BindingResult bindingResult) {
        HttpHeaders headers = new HttpHeaders();
        User savedUser = userService.storeUser(userConverter.convertToModel(user));
        if (savedUser != null)
            headers.add("UserId", savedUser.getId() + "");
        return new ResponseEntity<>(headers, HttpStatus.OK);
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public void handleErrors(Exception exception) {
        exception.printStackTrace();
    }
}
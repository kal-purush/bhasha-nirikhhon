package by.bsuir.tattoo4u.controller;

import by.bsuir.tattoo4u.dto.request.BanUserRequest;
import by.bsuir.tattoo4u.service.ServiceException;
import by.bsuir.tattoo4u.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/admin")
public class BanController {

    private UserService userService;

    @Autowired
    public BanController(UserService userService) {
        this.userService = userService;
    }

    @PostMapping("ban")
    @PreAuthorize("hasAuthority('ADMIN')")
    public ResponseEntity<?> banUser(@RequestBody @Validated BanUserRequest requestDto){

        if(requestDto==null){
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }

        try {
            userService.banUser(requestDto.getId());
        } catch (ServiceException e) {
            throw new ControllerException(e);
        }

        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PostMapping("unban")
    @PreAuthorize("hasAuthority('ADMIN')")
    public ResponseEntity<?> unbanUser(@RequestBody @Validated BanUserRequest requestDto){

        if(requestDto==null){
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }

        try {
            userService.unbanUser(requestDto.getId());
        } catch (ServiceException e) {
            throw new ControllerException(e);
        }

        return new ResponseEntity<>(HttpStatus.OK);
    }
}
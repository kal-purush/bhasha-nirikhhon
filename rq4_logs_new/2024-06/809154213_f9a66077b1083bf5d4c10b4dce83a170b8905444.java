package de.ait_tr.g_40_shop.controller;

import de.ait_tr.g_40_shop.domain.entity.User;
import de.ait_tr.g_40_shop.exception_handling.Response;
import de.ait_tr.g_40_shop.service.interfaces.UserService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/register")
public class RegistrationController {

    private final UserService service;

    public RegistrationController(UserService service) {
        this.service = service;
    }

    @PostMapping
    public Response register(@RequestBody User user) {
        service.register(user);
        return new Response("Registration complete. Please check your email.");
    }
}
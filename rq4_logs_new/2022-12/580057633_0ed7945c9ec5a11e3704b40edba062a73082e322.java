package com.vitweb.vitwebapi.adapter.web.v1.controller;

import com.vitweb.vitwebapi.adapter.web.base.RestApiV1;
import com.vitweb.vitwebapi.adapter.web.base.VsResponseUtil;
import com.vitweb.vitwebapi.adapter.web.v1.transfer.parameter.auth.AuthenticationRequest;
import com.vitweb.vitwebapi.application.constants.UrlConstant;
import com.vitweb.vitwebapi.application.events.SignUpEvent;
import com.vitweb.vitwebapi.application.inputs.user.CreateUserInput;
import com.vitweb.vitwebapi.application.services.IAuthService;
import com.vitweb.vitwebapi.domain.entities.User;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

@RestApiV1
public class AuthController {

    private final IAuthService authService;
    private final ApplicationEventPublisher publisher;

    public AuthController(IAuthService authService, ApplicationEventPublisher publisher) {
        this.authService = authService;
        this.publisher = publisher;
    }

    @PostMapping(UrlConstant.Auth.LOGIN)
    public ResponseEntity<?> login(@Valid @ModelAttribute AuthenticationRequest authenticationRequest) {
        return VsResponseUtil.ok(authService.login(authenticationRequest));
    }

    @PostMapping (UrlConstant.Auth.SIGNUP)
    public ResponseEntity<?> signUp(@Valid @ModelAttribute CreateUserInput createUserInput,
                                    HttpServletRequest request) {

        User user = authService.signUp(createUserInput);
        publisher.publishEvent(new SignUpEvent(
                user,
                applicationUrl(request)
        ));
        return VsResponseUtil.ok(user);
    }

    private String applicationUrl(HttpServletRequest request) {
        return "https://" +
                request.getServerName() +
                ":" +
                request.getServerPort() +
                request.getContextPath();
    }
}
package com.training.springbootbuyitem.component;

import com.training.springbootbuyitem.entity.model.User;
import com.training.springbootbuyitem.repository.UserRepository;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;

@Component
public class CurrentUserHelper {

    private final HttpServletRequest request;
    private final UserRepository userRepository;

    public CurrentUserHelper(HttpServletRequest request, UserRepository userRepository) {
        this.request = request;
        this.userRepository = userRepository;
    }

    public User getUser() {
        String email = getEmail();
        return userRepository.findByEmail(email);
    }

    public Long getUserUid() {
        User user = getUser();
        return user.getUserUid();
    }

    public String getEmail() {
        return this.request.getUserPrincipal().getName();
    }

}
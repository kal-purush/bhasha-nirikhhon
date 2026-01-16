package com.intouristing.intouristing.service;

import com.intouristing.intouristing.model.dto.UserDTO;
import com.intouristing.intouristing.model.dto.UserPositionDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.test.context.ActiveProfiles;

/**
 * Created by Marcelo Lacroix on 21/08/2019.
 */
@Service
@ActiveProfiles("webSocketTest")
public class UtilService {

    @Autowired
    UserService userService;

    public UserDTO buildUserDTO() {
        return UserDTO
                .builder()
                .username("testUsername")
                .password("testPassword")
                .userPosition(buildUserPositionDTO())
                .email("test_email@hotmail.com")
                .name("nameTest")
                .lastName("nameLastname")
                .build();
    }

    public UserPositionDTO buildUserPositionDTO() {
        return UserPositionDTO
                .builder()
                .accuracy(1.23456)
                .heading(22.222222)
                .latitude(33.333333)
                .longitude(21.121212)
                .speed(66.322212)
                .build();
    }

    public String usernamePasswordToLoginCredentials(String username, String password) {
        return String.format("{\"username\": \"%s\", \"password\": \"%s\"}", username, password);
    }

}
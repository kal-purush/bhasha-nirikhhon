package com.brainstrom.meokjang.authtest;

import com.brainstrom.meokjang.domain.User;
import com.brainstrom.meokjang.repository.UserRepository;
import com.brainstrom.meokjang.service.UserService;
import jakarta.persistence.EntityManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@Transactional
public class UserServiceTest {

    @Autowired UserRepository userRepository;
    @Autowired UserService userService;
    @Autowired EntityManager em;

    @Test
    void join() throws Exception {
        //given
        User user = new User();
        user.setUserName("test");
        user.setLocation("test");
        user.setLatitude(0.0);
        user.setLongitude(0.0);
        user.setGender(1);
        user.setReliability((float)50);

        //when
        long savedId = userService.join(user);

        //then
        assertEquals(user, userRepository.findById(savedId));
    }
}
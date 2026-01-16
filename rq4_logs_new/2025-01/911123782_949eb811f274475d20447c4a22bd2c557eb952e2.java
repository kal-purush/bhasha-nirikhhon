package com.roomfit.be;

import com.roomfit.be.user.application.dto.UserDTO;
import com.roomfit.be.user.application.UserService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
@Slf4j
public class UserTest {

    @Autowired
    private UserService userService;



    @Test
    public void 사용자_만들기_서비스(){
        UserDTO.Create request = new UserDTO.Create("testNick", "test@test.com", "password");
        UserDTO.Response response = userService.create(request);
        log.info(response.toString());
    }

    @Test
    public void 사용자_조회_서비스(){
        Long id = 1L;
        UserDTO.Response response = userService.readById(id);
        log.info(response.toString());
    }

    @Test
    public void 사용자_전체_조회_서비스(){
        List<UserDTO.Response> response = userService.readAll();
        log.info(response.toString());
    }

}

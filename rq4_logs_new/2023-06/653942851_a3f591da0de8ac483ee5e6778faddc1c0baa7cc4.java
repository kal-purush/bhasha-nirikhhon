package com.example.mealme.mapper;

import com.example.mealme.dto.UserDto;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
@Slf4j
class UserMapperTest {

    @Autowired
    private UserMapper userMapper;
    private UserDto userDto;

    @BeforeEach
    void setUp() {
        userDto = new UserDto();
        userDto.setUserId("aaa");
        userDto.setUserPassword("1234");
        userDto.setUserName("김바보");
        userDto.setUserNickname("바부");
        userDto.setUserGender("M");
        userDto.setUserPhoneNumber("01011112222");
        userDto.setUserAddressNumber(12345);
        userDto.setUserAddress1("경기도 안양시 동안구 학의로 111");
        userDto.setUserAddress2("201동 111호");
        userDto.setUserAddress3("삼성아파트");
        userDto.setUserEmail("aaa123@hanmail.net");
    }

    @Test
    @DisplayName("유저 회원가입 테스트")
    void insert() {
        userMapper.insert(userDto);

        Assertions.assertThat(userMapper.selectUserNumber(userDto.getUserId(), userDto.getUserPassword()))
                .isEqualTo(userDto.getUserNumber());

    }

    @Test
    void selectUserNumber() {
    }



}

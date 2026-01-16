package io.namoosori.rest.service.logic;

import io.namoosori.rest.entity.User;
import io.namoosori.rest.service.UserService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class UserServiceLogicTest {
    // 단위 테스트 DI는 생성자, 롬복 주입이 안됨
    @Autowired
    private UserService userService;

    private User kim;
    private User lee;

    @BeforeEach
    public void startUp() {
        System.out.println("테스트를 위한 초기화");

        kim = new User("Kim", "kim@abc.com");
        lee = new User("Lee", "lee@abc.com");

        this.userService.register(kim);
        this.userService.register(lee);
    }
    @Test
    public void registerTest() {
        User sample = User.sample();

        // junit 버전에 따란 다르게 테스트한다.
        assertThat(this.userService.register(sample)).isEqualTo(sample.getId());
        assertThat(this.userService.findAll().size()).isEqualTo(3);

        System.out.println("테스트로 추가한 데이터 제거");
        this.userService.remove(sample.getId());
    }

    @Test
    public void find() {
        assertThat(this.userService.find(lee.getId())).isNotNull();
        assertThat(this.userService.find(lee.getId()).getEmail()).isEqualTo(lee.getEmail());
    }

    @AfterEach
    public void cleanUp() {
        System.out.println("초기 데이터 삭제");
        this.userService.remove(kim.getId());
        this.userService.remove(lee.getId());
    }

}
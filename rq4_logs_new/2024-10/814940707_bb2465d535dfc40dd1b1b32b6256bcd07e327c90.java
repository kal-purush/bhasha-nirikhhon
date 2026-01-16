package bit.couple.service;

import static org.assertj.core.api.Assertions.*;

import bit.couple.domain.CoupleFixtures;
import bit.couple.dto.CoupleRequest;
import bit.user.domain.User;
import bit.user.dto.UserDto;
import bit.user.service.UserServiceImpl;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
class CoupleServiceTest {

    @Autowired
    CoupleService coupleService;

    @Autowired
    UserServiceImpl userService;

    @DisplayName("커플이 저장될 때, 유저의 정보가 update 되는지 확인한다.")
    @Commit
    @Transactional
    @Test
    void createCoupleCascadeUserUpdateTest() {
        // given
        List<User> users = CoupleFixtures.initialUsers();
        for (User user : users) {
            userService.create(UserDto.fromUser(user));
        }

        // when
        CoupleRequest coupleRequest = new CoupleRequest();
        coupleRequest.setUsers(users);
        coupleService.createCouple(coupleRequest.toCommand());
        User user = userService.getById(users.get(0).getId());

        // then
        assertThat(user.getCouple().getId()).isNotNull();
    }

}
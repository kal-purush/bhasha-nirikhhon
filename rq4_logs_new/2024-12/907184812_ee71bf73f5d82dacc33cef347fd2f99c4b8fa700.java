package tabom.myhands.user;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tabom.myhands.domain.user.entity.User;
import tabom.myhands.domain.user.repository.UserRepository;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class UserTest {

    @Autowired
    private UserRepository userRepository;

    @Test
    void saveAndFindById() {
        User user = User.builder()
                .departmentId(101)
                .name("김현지")
                .email("test@example.com")
                .password("password123")
                .photo("photo.jpg")
                .dayoffCnt(10.5f)
                .employeeNum(10001)
                .joinedAt(LocalDateTime.of(2024, 12, 20, 10, 0))
                .createdAt(LocalDateTime.now())
                .build();

        userRepository.save(user);
        Optional<User> foundUser = userRepository.findById(user.getUserId());

        assertThat(foundUser).isPresent();
        assertThat(foundUser.get().getName()).isEqualTo("김현지");
        assertThat(foundUser.get().getEmail()).isEqualTo("test@example.com");
    }
}
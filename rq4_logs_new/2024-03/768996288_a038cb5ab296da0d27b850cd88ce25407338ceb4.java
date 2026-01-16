package api.user;

import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@ActiveProfiles("test")
class UserRepositoryTest {
    @Autowired
    private UserRepository userRepository;

    @Test
    @DisplayName("유저 저장")
    public void save(){
        User user = new User("user@example.com", User.UserType.OWNER, "Abcd123@", "Abcd123@", User.Gender.MALE, LocalDateTime.now());
        User savedUser = userRepository.save(user);
        assertThat(savedUser.getEmail()).isEqualTo("user@example.com");
    }

    @Test
    @DisplayName("유저 조회")
    public void findById(){
        User user = new User("user@example.com", User.UserType.OWNER, "Abcd123@", "Abcd123@", User.Gender.MALE, LocalDateTime.now());
        User savedUser = userRepository.save(user);

        Optional<User> optionalUser = userRepository.findById(savedUser.getId());

        assertTrue(optionalUser.isPresent());
        assertEquals(savedUser.getEmail(), optionalUser.get().getEmail());
    }

    @Test
    @DisplayName("모든 유저 조회")
    public void findAll(){
        userRepository.save(new User("user1@example.com", User.UserType.OWNER, "Abcd123@", "Abcd123@", User.Gender.MALE, LocalDateTime.now()));
        userRepository.save(new User("user2@example.com", User.UserType.OWNER, "Abcd123@", "Abcd123@", User.Gender.MALE, LocalDateTime.now()));

        Iterable<User> users = userRepository.findAll();

        assertNotNull(users);
        assertThat(users).hasSizeGreaterThan(1);
    }

    @Test
    @DisplayName("유저 수 조회")
    public void count(){
        userRepository.save(new User("user1@example.com", User.UserType.OWNER, "Abcd123@", "Abcd123@", User.Gender.MALE, LocalDateTime.now()));
        userRepository.save(new User("user2@example.com", User.UserType.OWNER, "Abcd123@", "Abcd123@", User.Gender.MALE, LocalDateTime.now()));

        long count = userRepository.count();

        assertThat(count).isGreaterThanOrEqualTo(2);
    }

    @Test
    @DisplayName("유저 삭제")
    public void deleteById(){
        User user = new User("user@example.com", User.UserType.OWNER, "Abcd123@", "Abcd123@", User.Gender.MALE, LocalDateTime.now());
        User savedUser = userRepository.save(user);

        userRepository.deleteById(savedUser.getId());

        Optional<User> optionalUser = userRepository.findById(savedUser.getId());
        assertFalse(optionalUser.isPresent());
    }
    @Test
    @DisplayName("이메일로 유저 조회")
    public void findByEmail(){
        User user = new User("user@example.com", User.UserType.OWNER, "Abcd123@", "Abcd123@", User.Gender.MALE, LocalDateTime.now());
        userRepository.save(user);

        Optional<User> optionalUser = userRepository.findByEmail("user@example.com");

        assertTrue(optionalUser.isPresent());
        assertEquals(user.getEmail(), optionalUser.get().getEmail());
    }

    @Test
    @DisplayName("유저 타입으로 유저 조회")
    public void findByUserType(){
        User user1 = new User("user1@example.com", User.UserType.OWNER, "Abcd123@", "Abcd123@", User.Gender.MALE, LocalDateTime.now());
        User user2 = new User("user2@example.com", User.UserType.WALKER, "Abcd123@", "Abcd123@", User.Gender.FEMALE, LocalDateTime.now());
        userRepository.save(user1);
        userRepository.save(user2);

        List<User> owners = userRepository.findByUserType(User.UserType.OWNER);

        assertNotNull(owners);
        assertThat(owners).hasSizeGreaterThan(0);
        for (User owner : owners) {
            assertEquals(User.UserType.OWNER, owner.getUserType());
        }
    }
}
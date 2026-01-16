package api.userAccount;

import api.userAccount.enums.Gender;
import api.userAccount.enums.Role;
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
class UserAccountRepositoryTest {
    @Autowired
    private UserAccountRepository userAccountRepository;

    @Test
    @DisplayName("유저 저장")
    public void save(){
        UserAccount userAccount = new UserAccount("user@example.com", Role.OWNER, "Abcd123@", "Abcd123@", Gender.MALE, LocalDateTime.now());
        UserAccount savedUserAccount = userAccountRepository.save(userAccount);
        assertThat(savedUserAccount.getEmail()).isEqualTo("user@example.com");
    }

    @Test
    @DisplayName("유저 조회")
    public void findById(){
        UserAccount userAccount = new UserAccount("user@example.com", Role.OWNER, "Abcd123@", "Abcd123@", Gender.MALE, LocalDateTime.now());
        UserAccount savedUserAccount = userAccountRepository.save(userAccount);

        Optional<UserAccount> optionalUser = userAccountRepository.findById(savedUserAccount.getId());

        assertTrue(optionalUser.isPresent());
        assertEquals(savedUserAccount.getEmail(), optionalUser.get().getEmail());
    }

    @Test
    @DisplayName("모든 유저 조회")
    public void findAll(){
        userAccountRepository.save(new UserAccount("user1@example.com", Role.OWNER, "Abcd123@", "Abcd123@", Gender.MALE, LocalDateTime.now()));
        userAccountRepository.save(new UserAccount("user2@example.com", Role.OWNER, "Abcd123@", "Abcd123@", Gender.MALE, LocalDateTime.now()));

        Iterable<UserAccount> users = userAccountRepository.findAll();

        assertNotNull(users);
        assertThat(users).hasSizeGreaterThan(1);
    }

    @Test
    @DisplayName("유저 수 조회")
    public void count(){
        userAccountRepository.save(new UserAccount("user1@example.com", Role.OWNER, "Abcd123@", "Abcd123@", Gender.MALE, LocalDateTime.now()));
        userAccountRepository.save(new UserAccount("user2@example.com", Role.OWNER, "Abcd123@", "Abcd123@", Gender.MALE, LocalDateTime.now()));

        long count = userAccountRepository.count();

        assertThat(count).isGreaterThanOrEqualTo(2);
    }

    @Test
    @DisplayName("유저 삭제")
    public void deleteById(){
        UserAccount userAccount = new UserAccount("user@example.com", Role.OWNER, "Abcd123@", "Abcd123@", Gender.MALE, LocalDateTime.now());
        UserAccount savedUserAccount = userAccountRepository.save(userAccount);

        userAccountRepository.deleteById(savedUserAccount.getId());

        Optional<UserAccount> optionalUser = userAccountRepository.findById(savedUserAccount.getId());
        assertFalse(optionalUser.isPresent());
    }
    @Test
    @DisplayName("이메일로 유저 조회")
    public void findByEmail(){
        UserAccount userAccount = new UserAccount("user@example.com", Role.OWNER, "Abcd123@", "Abcd123@", Gender.MALE, LocalDateTime.now());
        userAccountRepository.save(userAccount);

        Optional<UserAccount> optionalUser = userAccountRepository.findByEmail("user@example.com");

        assertTrue(optionalUser.isPresent());
        assertEquals(userAccount.getEmail(), optionalUser.get().getEmail());
    }

    @Test
    @DisplayName("유저 타입으로 유저 조회")
    public void findByUserType(){
        UserAccount userAccount1 = new UserAccount("user1@example.com", Role.OWNER, "Abcd123@", "Abcd123@", Gender.MALE, LocalDateTime.now());
        UserAccount userAccount2 = new UserAccount("user2@example.com", Role.WALKER, "Abcd123@", "Abcd123@", Gender.FEMALE, LocalDateTime.now());
        userAccountRepository.save(userAccount1);
        userAccountRepository.save(userAccount2);

        List<UserAccount> owners = userAccountRepository.findByUserType(Role.OWNER);

        assertNotNull(owners);
        assertThat(owners).hasSizeGreaterThan(0);
        for (UserAccount owner : owners) {
            assertEquals(Role.OWNER, owner.getRole());
        }
    }
}
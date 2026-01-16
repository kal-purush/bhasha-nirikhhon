package api.dog;

import api.user.User;
import api.user.UserRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.List;


@DataJpaTest
@ActiveProfiles("test")
public class DogRepositoryTest {
    @Autowired
    private DogRepository dogRepository;

    @Autowired
    private UserRepository userRepository;

    @Test
    @DisplayName("dog 저장")
    public void save(){
        User user = new User("user@example.com", User.UserType.OWNER, "Abcd123@", "Abcd123@", User.Gender.MALE, LocalDateTime.now());
        user = userRepository.save(user);

        Dog dog = new Dog("testDogName", user);
        Dog savedDog = dogRepository.save(dog);

        assertThat(savedDog.getDogName()).isEqualTo("testDogName");
    }

    @Test
    @DisplayName("Owner가 소유한 dog 조회")
    public void findDogsByOwner(){
        User owner = new User("user@example.com", User.UserType.OWNER, "Abcd123@", "Abcd123@", User.Gender.MALE, LocalDateTime.now());
        owner = userRepository.save(owner);

        Dog dog1 = new Dog("testDog1", owner);
        dogRepository.save(dog1);

        Dog dog2 = new Dog("testDog2", owner);
        dogRepository.save(dog2);

        List<Dog> dogsByOwner = dogRepository.findByOwner(owner);

        assertThat(dogsByOwner).isNotEmpty();
        assertThat(dogsByOwner.get(0)).isEqualTo(dog1);
        assertThat(dogsByOwner.get(1)).isEqualTo(dog2);
    }
}
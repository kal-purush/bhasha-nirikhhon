import com.SkillBox.users.UsersApplication;
import com.SkillBox.users.entity.Gender;
import com.SkillBox.users.entity.Subscription;
import com.SkillBox.users.entity.User;
import com.SkillBox.users.repository.GenderRepository;
import com.SkillBox.users.repository.SubscriptionRepository;
import com.SkillBox.users.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

@Testcontainers(disabledWithoutDocker = true)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = UsersApplication.class)
public class SubscriptionModuleTest {

    private final static String MALE_GENDER_DESCRIPTION = "Male";
    private final static Gender MALE_GENDER = new Gender(1L, MALE_GENDER_DESCRIPTION);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private SubscriptionRepository subscriptionRepository;

    @Autowired
    private GenderRepository genderRepository;

    @BeforeEach
    public void cleanDB() {
        userRepository.deleteAll();
        subscriptionRepository.deleteAll();
    }

    @Container
    private static final PostgreSQLContainer<PostgresContainerWrapper> postgresContainer = new PostgresContainerWrapper();

    @DynamicPropertySource
    public static void initSystemParams(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgresContainer::getJdbcUrl);
        registry.add("spring.datasource.username", postgresContainer::getUsername);
        registry.add("spring.datasource.password", postgresContainer::getPassword);
    }

    @Test
    public void get_user_subscription_success() throws Exception {
        // given
        User user1 = getCommonUser();
        user1.setId(1L);
        user1.setUserNickname("some");
        user1.setEmail("some");

        User user2 = getCommonUser();
        user2.setId(2L);
        user2.setUserNickname("some1");
        user2.setEmail("some1");

        List<User> users = List.of(user1, user2);

        Subscription subscription = new Subscription();
        subscription.setUserId1(1L);
        subscription.setUserId2(2L);

        genderRepository.save(MALE_GENDER);
        userRepository.saveAll(users);
        subscriptionRepository.save(subscription);

        // when/then
        mockMvc.perform(MockMvcRequestBuilders.get("/subscription/{userId}", 1L))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(MockMvcResultMatchers.jsonPath("$").isArray())
                .andExpect(MockMvcResultMatchers.jsonPath("$.length()").value(1));
    }

    @Test
    public void create_user_subscription_success() throws Exception {
        // given
        User user1 = getCommonUser();
        user1.setId(1L);
        user1.setUserNickname("some");
        user1.setEmail("some");

        User user2 = getCommonUser();
        user2.setId(2L);
        user2.setUserNickname("some1");
        user2.setEmail("some1");

        List<User> users = List.of(user1, user2);

        genderRepository.save(MALE_GENDER);
        userRepository.saveAll(users);

        // when/then
        mockMvc.perform(MockMvcRequestBuilders.post("/subscription/{userId1}/{userId2}", 1L, 2L))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(MockMvcResultMatchers.jsonPath("$.userId1").value(1L))
                .andExpect(MockMvcResultMatchers.jsonPath("$.userId2").value(2L));
    }

    @Test
    public void delete_user_subscription_success() throws Exception {
        // given
        User user1 = getCommonUser();
        user1.setId(1L);
        user1.setUserNickname("some");
        user1.setEmail("some");

        User user2 = getCommonUser();
        user2.setId(2L);
        user2.setUserNickname("some1");
        user2.setEmail("some1");

        List<User> users = List.of(user1, user2);

        Subscription subscription = new Subscription();
        subscription.setUserId1(1L);
        subscription.setUserId2(2L);

        genderRepository.save(MALE_GENDER);
        userRepository.saveAll(users);
        subscriptionRepository.save(subscription);

        // when/then
        mockMvc.perform(MockMvcRequestBuilders.delete("/subscription/{userId1}/{userId2}", 1L, 2L))
                .andExpect(MockMvcResultMatchers.status().isNoContent());
    }

    private User getCommonUser() {
        User user = new User();
        user.setFirstName("Ivan");
        user.setLastName("Ivanov");
        user.setGender(new Gender(1L, MALE_GENDER_DESCRIPTION));
        user.setUserNickname("Ivan");
        user.setEmail("Ivan@.com");
        return user;
    }
}
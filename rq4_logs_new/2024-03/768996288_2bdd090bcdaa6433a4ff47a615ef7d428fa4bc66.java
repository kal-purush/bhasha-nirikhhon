package BE;



import BE.repository.UserRepository;
import BE.service.AuthService;
import jakarta.persistence.EntityManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class BeConfig {
    private final DataSource dataSource;
    private final EntityManager em;
    public BeConfig(DataSource dataSource, EntityManager em){
        this.dataSource = dataSource;
        this.em = em;
    }
    @Bean
    public AuthService authService(){
        return new AuthService(userRepository());
    }

    @Bean
    public UserRepository userRepository(){
        return new UserRepository(em);
    }
}

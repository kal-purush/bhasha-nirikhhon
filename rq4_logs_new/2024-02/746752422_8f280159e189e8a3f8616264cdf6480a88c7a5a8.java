package example.spring.configuration;

import example.spring.model.Account;
import example.spring.model.enams.Gender;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDate;
import java.time.Month;
import java.util.Arrays;
import java.util.List;

@Configuration
public class AccountsConfig {
    @Bean
    public List<Account> accounts() {
        return Arrays.asList(
                new Account("John", "Doe", "USA", LocalDate.of(1990, Month.JANUARY, 12), 16000d, Gender.MALE),
                new Account("Alice", "Johnson", "Canada", LocalDate.of(1995, Month.MARCH, 5), 18000d, Gender.FEMALE),
                new Account("Bob", "Smith", "UK", LocalDate.of(2000, Month.AUGUST, 22), 13000d, Gender.MALE),
                new Account("Emily", "Clark", "Germany", LocalDate.of(1992, Month.JULY, 15), 12000d, Gender.FEMALE)
        );
    }
}
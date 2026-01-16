package atm;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AtmConfig {

    @Bean
    public DataSource dataSource() {
        return new DataSourceFile("customers.txt");
    }

    @Bean
    public Bank bank() {
        return new Bank("My Bank", dataSource());
    }

    @Bean
    public ATM atm() {
        return new ATM(bank());
    }

    @Bean
    public AtmUI atmUI() {
        return new AtmUI(atm());
    }
}
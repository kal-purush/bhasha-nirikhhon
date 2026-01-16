package cz.vlasec.bitcoinxrate;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableJpaRepositories("cz.vlasec.bitcoinxrate.server.orm.dao")
@EnableScheduling
//@Sql
public class Launcher {
	public static void main(String[] args) {
		SpringApplication.run(Launcher.class, args);
	}
}
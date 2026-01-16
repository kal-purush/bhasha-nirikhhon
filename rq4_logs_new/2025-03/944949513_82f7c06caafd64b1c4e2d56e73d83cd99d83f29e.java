package le.ac.uk;

import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;


@Slf4j
@MapperScan("le.ac.uk.mapper")
@SpringBootApplication(scanBasePackages = {"le.ac.uk"},exclude={DataSourceAutoConfiguration.class})
public class MainApplication {
    public static void main(String[] args) {
        SpringApplication.run(MainApplication.class, args);
        log.info("MainApplication start success");
    }
}
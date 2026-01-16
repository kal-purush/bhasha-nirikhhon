package ruangong.our_land;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@EnableAspectJAutoProxy
@SpringBootApplication
@MapperScan("ruangong.our_land.mapper")
public class Ruangong4Application {

    public static void main(String[] args) {
        SpringApplication.run(Ruangong4Application.class, args);
    }

}
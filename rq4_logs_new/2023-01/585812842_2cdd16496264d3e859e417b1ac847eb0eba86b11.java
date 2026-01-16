package study.mybatis.mybatis_plus_datasource;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("study.mybatis.mybatis_plus_datasource.mapper")
public class MybatisPlusDatasourceApplication {

  public static void main(String[] args) {
    SpringApplication.run(MybatisPlusDatasourceApplication.class, args);
  }

}
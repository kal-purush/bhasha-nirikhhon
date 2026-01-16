package org.singhlee.admin;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * springboot启动类
 * @author singhlee
 */
@Slf4j
@SpringBootApplication
public class AdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(AdminApplication.class, args);
        log.info("==================管理系统启动成功================");
        log.info("==================当前项目目录：{}================", System.getProperty("user.dir"));
    }

}
package com.baizhi;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication //开启SpringBoot应用
@MapperScan("com.baizhi.Dao")  //自动创建DAO实现类对象
//*@ComponentScan(basePackages = "com.baizhi.*") 扫描入口类所在子包以及子包后代包中注解(也可以从新指定要扫描哪些包)
public class Application {

    public static void main(String[] args) {
        //启动SpringBoot应用
        SpringApplication.run(Application.class,args);
    }
}
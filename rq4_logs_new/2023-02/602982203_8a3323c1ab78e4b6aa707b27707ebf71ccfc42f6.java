package com.joizhang.naiverpc.springboot.demo.consumer;

import com.joizhang.naiverpc.spring.annotation.NaiveRpcReference;
import com.joizhang.naiverpc.spring.context.annotation.EnableNaiveRpc;
import com.joizhang.naiverpc.springboot.demo.HelloService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Service;

@SpringBootApplication
@Service
@EnableNaiveRpc
public class ConsumerApplication {
    @NaiveRpcReference
    private HelloService helloService;

    public String doSayHello(String name) {
        return helloService.hello(name);
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(ConsumerApplication.class, args);
        ConsumerApplication application = context.getBean(ConsumerApplication.class);
        String result = application.doSayHello("world");
        System.out.println("result: " + result);
    }
}
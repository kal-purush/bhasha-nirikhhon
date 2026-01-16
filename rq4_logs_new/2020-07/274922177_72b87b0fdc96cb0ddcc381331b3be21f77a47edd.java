package com.center.service.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * @author
 * @Title:
 * @Package com.center.service.runner
 * @Description:
 * @Company
 * @date 2020/7/20 17:55
 */

@Component
public class ShiroApplicationRunner implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(ShiroApplicationRunner.class);

    @Value("${server.port}")
    private int port;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info("程序部署完成，访问地址：http://localhost:" + port);
    }
}
package james.web.template.controller;

import james.web.template.controller.base.BaseController;
import james.web.template.service.ITestService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/redis/test")
public class RedisTemplateController extends BaseController {
    private static final Logger logger = LoggerFactory.getLogger(RedisTemplateController.class);

    // @Autowired
    // IRedisService redisService;

    @Autowired
    ITestService testService;

    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public void loadAlldatatable() {
        logger.info("redis/test/test() called");
        System.out.println("redis/test/test() called");

        // redisClientTemplate.set("a", "abc");
        // System.out.println(redisClientTemplate.get("a"));
    }
}
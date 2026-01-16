package top.codingshen.middleware.dynamic.thread.pool.sdk.config;

import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @ClassName DynamicThreadPoolAutoConfig
 * @Description 动态配置入口
 * @Author alex_shen
 * @Date 2024/8/3 - 17:44
 */
@Configuration
public class DynamicThreadPoolAutoConfig {

    private final Logger logger = LoggerFactory.getLogger(DynamicThreadPoolAutoConfig.class);

    @Bean("dynamicThreadPoolService")
    public String dynamicThreadPoolService(ApplicationContext applicationContext, Map<String, ThreadPoolExecutor> threadPoolExecutorMap) {

        String applicationName = applicationContext.getEnvironment().getProperty("spring.application.name");

        if (StringUtils.isBlank(applicationName)) {
            applicationName = "dynamic-thread-pool-test-default";
            logger.warn("动态线程池, 启动提示, SpringBoot 应用未配置 spring.application.name 无法获取到应用名称");
        }

        logger.info("线程池信息: {}", JSON.toJSONString(threadPoolExecutorMap.keySet()));

        return new String();
    }

}
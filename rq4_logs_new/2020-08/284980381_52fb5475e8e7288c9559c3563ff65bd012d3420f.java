package com.autohome.server.config;

import com.netflix.loadbalancer.AvailabilityFilteringRule;
import com.netflix.loadbalancer.IRule;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * @program: OpenMain
 * @description:
 * @author: Feri(邢朋辉)
 * @create: 2020-07-28 20:29
 */
@Configuration
public class RibbonConfig {
    @Bean
    @LoadBalanced //启用Ribbon的负载均衡
    public RestTemplate createRT() {
        return new RestTemplate();
    }

    //Ribbon的负载均衡策略算法
    @Bean
    public IRule createRule() {
        //return new RandomRule();//随机策略
        //return new ZoneAvoidanceRule();//区域感知 就近访问
        //return new WeightedResponseTimeRule();//权重响应时间策略
        return new AvailabilityFilteringRule(); //可用性策略，会选择可用性服务提供者
        //return new BestAvailableRule(); //并发量最小策略
        //return new RoundRobinRule();//轮训策略
    }
}
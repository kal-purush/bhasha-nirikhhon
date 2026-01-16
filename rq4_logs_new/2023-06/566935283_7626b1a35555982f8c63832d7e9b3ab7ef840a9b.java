/*
 * Copyright 2015-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.gemfire.spring.session.client;

import org.apache.geode.cache.client.ClientRegionShortcut;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.gemfire.config.annotation.ClientCacheApplication;
import org.springframework.data.gemfire.config.annotation.ClientCacheConfigurer;
import org.springframework.data.gemfire.support.ConnectionEndpoint;
import org.springframework.session.data.gemfire.config.annotation.web.http.EnableGemFireHttpSession;

@SpringBootApplication
@ClientCacheApplication(name = "SimpleSpringSessionGemFireSampleClient",
        readTimeout = 15000, retryAttempts = 1, subscriptionEnabled = true)
@EnableGemFireHttpSession(poolName = "DEFAULT",clientRegionShortcut = ClientRegionShortcut.CACHING_PROXY)
public class ClientConfig {

    public static void main(String[] args) {
        new AnnotationConfigApplicationContext(ClientConfig.class);
    }

    @Bean
    ClientCacheConfigurer gemfireServerReadyConfigurer(@Value("${spring.data.gemfire.cache.server.port:40404}") int cacheServerPort) {

        return (beanName, clientCacheFactoryBean) -> clientCacheFactoryBean.addServers(new ConnectionEndpoint("localhost", cacheServerPort));
    }
}
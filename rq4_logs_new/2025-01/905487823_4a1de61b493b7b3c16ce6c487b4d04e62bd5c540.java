package br.ufrn.FormsFramework.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
public class ScanConfig {

    @Profile("DASH")
    @Configuration
    @ComponentScan(basePackages = {"br.ufrn.FormsFramework","br.ufrn.instancies.DASH"})
    class DashConfig {

    }

    @Profile("GRAB")
    @Configuration
    @ComponentScan(basePackages = {"br.ufrn.FormsFramework","br.ufrn.instancies.GRAB"})
    class GrabConfig {

    }
    
    @Profile("JUMP")
    @Configuration
    @ComponentScan(basePackages = {"br.ufrn.FormsFramework","br.ufrn.instancies.JUMP"})
    class JumpConfig {

    }
}
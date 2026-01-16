package com.spring.mood.projectmvc.config.queryDslCoonfig;


import com.querydsl.jpa.impl.JPAQueryFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

@Configuration
public class QueryDslConfig {
    @PersistenceContext
    private EntityManager em;

    @Bean // 외부라이브러리르 스프링 컨테이너에 관리시키는 설정
    public JPAQueryFactory jpaQueryFactory() {
        return new JPAQueryFactory(em);
    }
}